import logging
import subprocess
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import S3Uploader, prepare_s3_bucket


REST_URL = "http://rest:8181/v1"
NAMESPACE = "default"

MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "ClickHouse_Minio_P@ssw0rd"
WAREHOUSE_BUCKET = "warehouse-rest"

CATALOG_DATABASE = "iceberg_trino_test"


def _get_uuid_str() -> str:
    return str(uuid.uuid4()).replace("-", "_")


@pytest.fixture(scope="package")
def started_cluster_with_trino():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node1",
        main_configs=[
            "configs/config.d/cluster.xml",
            "configs/config.d/named_collections.xml",
            "configs/config.d/query_log.xml",
        ],
        user_configs=["configs/users.d/users.xml"],
        stay_alive=True,
        with_iceberg_catalog=True,
        extra_parameters={
            "docker_compose_file_name": "docker_compose_iceberg_rest_catalog_with_trino.yml",
        },
    )

    try:
        logging.info("Starting cluster with Trino...")
        cluster.start()

        # Wait for Trino health-check + REST catalog readiness. The compose health-check
        # only guards the `rest` service for `depends_on`; Trino itself becomes ready a
        # few seconds after the container starts. A brief warm-up avoids races on the
        # first SELECT.
        _wait_for_trino_ready(cluster, timeout_seconds=120)

        yield cluster
    finally:
        cluster.shutdown()


def _trino_container_name(cluster: ClickHouseCluster) -> str:
    """Trino service container created by docker compose."""
    project = cluster.project_name
    return f"{project}-trino-1"


def _trino_exec(cluster: ClickHouseCluster, sql: str) -> str:
    """Run a SQL statement in Trino via the bundled `trino` CLI inside the container."""
    container = _trino_container_name(cluster)
    proc = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "trino",
            "--server",
            "http://localhost:8080",
            "--catalog",
            "iceberg",
            "--schema",
            NAMESPACE,
            "--output-format",
            "TSV",
            "--execute",
            sql,
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Trino query failed (exit {proc.returncode}):\n"
            f"  SQL: {sql}\n  stdout: {proc.stdout}\n  stderr: {proc.stderr}"
        )
    return proc.stdout


def _wait_for_trino_ready(cluster: ClickHouseCluster, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            out = _trino_exec(cluster, "SELECT 1")
            if out.strip() == "1":
                return
        except Exception as e:
            last_err = e
        time.sleep(2)
    raise RuntimeError(f"Trino did not become ready in {timeout_seconds}s: {last_err}")


def _create_clickhouse_iceberg_database(node, name: str) -> None:
    node.query(f"DROP DATABASE IF EXISTS {name}")
    node.query(
        f"""
        CREATE DATABASE {name} ENGINE = DataLakeCatalog('{REST_URL[:-3]}', '{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}')
        SETTINGS
            catalog_type = 'rest',
            warehouse = 's3://{WAREHOUSE_BUCKET}/',
            storage_endpoint = 'http://minio:9000/{WAREHOUSE_BUCKET}'
        """,
        settings={
            "allow_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )


def test_clickhouse_writes_geometry_trino_reads(started_cluster_with_trino):
    cluster = started_cluster_with_trino
    node = cluster.instances["node1"]

    _create_clickhouse_iceberg_database(node, CATALOG_DATABASE)

    table_name = f"geom_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    geo_settings = {
        "allow_experimental_geo_types_in_iceberg": 1,
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, geom Geometry)
        ENGINE = IcebergS3('http://minio:9000/{WAREHOUSE_BUCKET}/{table_name}/', '{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}')
        SETTINGS iceberg_format_version = 3
        """,
        settings=geo_settings,
    )

    node.query(
        f"""
        INSERT INTO {full}
        SELECT 1, readWkt('POINT(1 2)') UNION ALL
        SELECT 2, readWkt('LINESTRING(0 0, 1 1, 2 2)') UNION ALL
        SELECT 3, readWkt('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') UNION ALL
        SELECT 4, readWkt('MULTILINESTRING((0 0, 1 1),(2 2, 3 3))') UNION ALL
        SELECT 5, readWkt('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)),((2 2, 3 2, 3 3, 2 2)))')
        """,
        settings=geo_settings,
    )

    expected_wkt = {
        1: "POINT (1 2)",
        2: "LINESTRING (0 0, 1 1, 2 2)",
        3: "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
        4: "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
        5: "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
    }

    # ST_AsText on the geometry column gives a canonical WKT we can compare against.
    out = _trino_exec(
        cluster,
        f'SELECT id, ST_AsText(geom) FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    lines = [line for line in out.strip().splitlines() if line]
    assert len(lines) == 5, f"expected 5 rows from Trino, got {len(lines)}:\n{out}"

    for line in lines:
        id_str, wkt = line.split("\t", 1)
        row_id = int(id_str)
        assert wkt.strip() == expected_wkt[row_id], (
            f"row id={row_id}: expected {expected_wkt[row_id]!r}, got {wkt!r}"
        )
