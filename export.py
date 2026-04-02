"""
Export all tables from a PostgreSQL database to Parquet files.

Uses psycopg2 for Postgres, pyarrow for Parquet writing.
No DuckDB extensions, no ODBC drivers.
"""

import os
import sys
import time
import logging
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pg-parquet-export")


def get_config() -> dict:
    required = ["PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DATABASE"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error(f"Missing required env vars: {', '.join(missing)}")
        sys.exit(1)

    compression = os.environ.get("EXPORT_COMPRESSION", "zstd")
    if compression == "none":
        compression = None

    return {
        "host": os.environ["PG_HOST"],
        "port": os.environ["PG_PORT"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "database": os.environ["PG_DATABASE"],
        "schemas": [
            s.strip()
            for s in os.environ.get("EXPORT_SCHEMAS", "public").split(",")
            if s.strip()
        ],
        "compression": compression,
        "output_dir": Path(os.environ.get("EXPORT_OUTPUT_DIR", "/app/output")),
    }


def connect_postgres(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
    )


def discover_tables(pg_conn, schemas: list[str]) -> list[tuple[str, str]]:
    schema_list = ", ".join(f"'{s}'" for s in schemas)
    cur = pg_conn.cursor()
    cur.execute(
        f"SELECT table_schema, table_name "
        f"FROM information_schema.tables "
        f"WHERE table_type = 'BASE TABLE' "
        f"AND table_schema IN ({schema_list}) "
        f"ORDER BY table_schema, table_name"
    )
    rows = cur.fetchall()
    cur.close()

    log.info(f"Discovered {len(rows)} table(s) across schema(s): {', '.join(schemas)}")
    return rows


def export_table(
    pg_conn,
    schema: str,
    table: str,
    output_dir: Path,
    compression: str | None,
) -> dict:
    out_path = output_dir / schema / f"{table}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()

    cur = pg_conn.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}"')
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()

    # Transpose rows into column arrays for pyarrow
    col_data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    arrow_table = pa.table(col_data)
    pq.write_table(arrow_table, str(out_path), compression=compression)

    elapsed = time.perf_counter() - t0
    size_mb = out_path.stat().st_size / (1024 * 1024)

    return {
        "schema": schema,
        "table": table,
        "path": str(out_path),
        "size_mb": round(size_mb, 2),
        "seconds": round(elapsed, 2),
        "rows": len(rows),
    }


def main():
    cfg = get_config()
    cfg["output_dir"].mkdir(parents=True, exist_ok=True)

    log.info(
        f"Exporting {cfg['database']} -> Parquet "
        f"(schemas={cfg['schemas']}, compression={cfg['compression']})"
    )

    pg_conn = connect_postgres(cfg)
    log.info("Connected to PostgreSQL")

    try:
        tables = discover_tables(pg_conn, cfg["schemas"])

        if not tables:
            log.warning("No tables found. Check EXPORT_SCHEMAS is correct.")
            return

        results = []
        errors = []

        for schema, table in tables:
            try:
                meta = export_table(
                    pg_conn, schema, table, cfg["output_dir"], cfg["compression"]
                )
                results.append(meta)
                log.info(
                    f"  OK {schema}.{table} -> "
                    f"{meta['rows']} rows, {meta['size_mb']} MB "
                    f"({meta['seconds']}s)"
                )
            except Exception as e:
                errors.append((schema, table, str(e)))
                log.error(f"  FAIL {schema}.{table} -- {e}")

        log.info("-" * 60)
        total_mb = sum(r["size_mb"] for r in results)
        total_s = sum(r["seconds"] for r in results)
        total_rows = sum(r["rows"] for r in results)
        log.info(
            f"Done: {len(results)}/{len(tables)} tables exported, "
            f"{total_rows} rows, {total_mb:.1f} MB total, {total_s:.1f}s elapsed"
        )

        if errors:
            log.warning(f"{len(errors)} table(s) failed:")
            for schema, table, err in errors:
                log.warning(f"  - {schema}.{table}: {err}")
            sys.exit(1)

    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
