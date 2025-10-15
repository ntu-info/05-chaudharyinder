# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])

    # ...existing code...

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a: str, term_b: str):
        """
        Return studies that mention term_a but not term_b (A\B), and the reverse (B\A).
        Terms are compared lowercase with underscores and a database-specific prefix.
        """
        # CORRECTED: Add the prefix to match the database format
        a = f"terms_abstract_tfidf__{term_a.strip().lower().replace(' ', '_')}"
        b = f"terms_abstract_tfidf__{term_b.strip().lower().replace(' ', '_')}"
    
        eng = get_engine()

        sql = text("""
            WITH a AS (
                SELECT DISTINCT study_id
                FROM ns.annotations_terms
                WHERE term = :a AND weight > 0
            ),
            b AS (
                SELECT DISTINCT study_id
                FROM ns.annotations_terms
                WHERE term = :b AND weight > 0
            ),
            a_not_b AS (
                SELECT study_id FROM a
                WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.study_id = a.study_id)
            ),
            b_not_a AS (
                SELECT study_id FROM b
                WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.study_id = b.study_id)
            )
            SELECT 'a_not_b' AS kind, study_id FROM a_not_b
            UNION ALL
            SELECT 'b_not_a' AS kind, study_id FROM b_not_a
            ORDER BY 1, 2;
        """)

        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            rows = conn.execute(sql, {"a": a, "b": b}).mappings().all()

        a_nb = [r["study_id"] for r in rows if r["kind"] == "a_not_b"]
        b_na = [r["study_id"] for r in rows if r["kind"] == "b_not_a"]

        return jsonify({
            "a": a,
            "b": b,
            "a_not_b": a_nb,
            "b_not_a": b_na,
            "counts": {"a_not_b": len(a_nb), "b_not_a": len(b_na)}
        })

    @app.get("/dissociate/locations/<coord1>/<coord2>", endpoint="dissociate_locations")
    def dissociate_locations(coord1: str, coord2: str):
        """
        Return studies that report coord1 but not coord2 (A\B), and the reverse (B\A).
        Coordinates use x_y_z (integers). Matching is done on rounded coordinates.
        """
        def parse_coords(s: str):
            try:
                x, y, z = [int(p) for p in s.split("_")]
                return x, y, z
            except Exception:
                abort(400, description="Coordinates must be 'x_y_z' with integers.")

        (x1, y1, z1) = parse_coords(coord1)
        (x2, y2, z2) = parse_coords(coord2)

        eng = get_engine()
        sql = text("""
            WITH a AS (
                SELECT DISTINCT study_id
                FROM ns.coordinates
                WHERE round(ST_X(geom)) = :x1
                  AND round(ST_Y(geom)) = :y1
                  AND round(ST_Z(geom)) = :z1
            ),
            b AS (
                SELECT DISTINCT study_id
                FROM ns.coordinates
                WHERE round(ST_X(geom)) = :x2
                  AND round(ST_Y(geom)) = :y2
                  AND round(ST_Z(geom)) = :z2
            ),
            a_not_b AS (
                SELECT study_id FROM a
                WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.study_id = a.study_id)
            ),
            b_not_a AS (
                SELECT study_id FROM b
                WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.study_id = b.study_id)
            )
            SELECT 'a_not_b' AS kind, study_id FROM a_not_b
            UNION ALL
            SELECT 'b_not_a' AS kind, study_id FROM b_not_a
            ORDER BY 1, 2;
        """)

        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            rows = conn.execute(sql, {
                "x1": x1, "y1": y1, "z1": z1,
                "x2": x2, "y2": y2, "z2": z2
            }).mappings().all()

        a_nb = [r["study_id"] for r in rows if r["kind"] == "a_not_b"]
        b_na = [r["study_id"] for r in rows if r["kind"] == "b_not_a"]

        return jsonify({
            "a": {"x": x1, "y": y1, "z": z1},
            "b": {"x": x2, "y": y2, "z": z2},
            "a_not_b": a_nb,
            "b_not_a": b_na,
            "counts": {"a_not_b": len(a_nb), "b_not_a": len(b_na)}
        })

    # ...existing code...

    @app.get("/test_db", endpoint="test_db")
    
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point (no __main__)
app = create_app()