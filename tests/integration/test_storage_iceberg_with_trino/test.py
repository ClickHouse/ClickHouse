import logging
import subprocess
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


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
        _wait_for_trino_ready(cluster, timeout_seconds=120)

        yield cluster
    finally:
        cluster.shutdown()


def _trino_container_name(cluster: ClickHouseCluster) -> str:
    """Trino service container created by docker compose."""
    project = cluster.project_name
    return f"{project}-trino-1"


def _trino_exec(cluster: ClickHouseCluster, sql: str) -> str:
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
            out = _trino_exec(
                cluster,
                "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'",
            )
            if out.strip().isdigit() and int(out.strip()) >= 1:
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


WRITE_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "write_full_path_in_iceberg_metadata": 1,
}


@pytest.fixture(scope="package")
def iceberg_db(started_cluster_with_trino):
    """Create the ClickHouse DataLakeCatalog database once per test session."""
    node = started_cluster_with_trino.instances["node1"]
    _create_clickhouse_iceberg_database(node, CATALOG_DATABASE)
    return started_cluster_with_trino


def _engine_clause(table_name: str) -> str:
    return (
        f"ENGINE = IcebergS3('http://minio:9000/{WAREHOUSE_BUCKET}/{table_name}/', "
        f"'{MINIO_ACCESS_KEY}', '{MINIO_SECRET_KEY}')"
    )


def test_clickhouse_writes_trino_reads(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"basic_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, name String, value Float64)
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )

    node.query(
        f"""
        INSERT INTO {full} VALUES
            (1, 'alpha', 1.5),
            (2, 'beta',  2.5),
            (3, 'gamma', 3.25)
        """,
        settings=WRITE_SETTINGS,
    )

    out = _trino_exec(
        cluster,
        f'SELECT id, name, value FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    expected = "1\talpha\t1.5\n2\tbeta\t2.5\n3\tgamma\t3.25\n"
    assert out == expected, f"expected {expected!r}, got {out!r}"


def test_complex_types(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"complex_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (
            id Int32,
            arr Array(Int64),
            m Map(String, Int32),
            s Tuple(x Int32, y String)
        )
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )

    node.query(
        f"""
        INSERT INTO {full} VALUES
            (1, [10, 20, 30], map('a', 1, 'b', 2), (7, 'seven')),
            (2, [],            map(),               (0, ''))
        """,
        settings=WRITE_SETTINGS,
    )

    out = _trino_exec(
        cluster,
        f"""
        SELECT
            id,
            cardinality(arr),
            element_at(arr, 1),
            element_at(m, 'a'),
            element_at(m, 'b'),
            s.x,
            s.y
        FROM "{NAMESPACE}"."{table_name}"
        ORDER BY id
        """,
    )
    expected = "1\t3\t10\t1\t2\t7\tseven\n2\t0\t\t\t\t0\t\n"
    assert out == expected, f"complex types: expected {expected!r}, got {out!r}"


def test_partition_pruning(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"part_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, region String, value Int64)
        {_engine_clause(table_name)}
        PARTITION BY identity(region)
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )

    node.query(
        f"""
        INSERT INTO {full} VALUES
            (1, 'us', 10), (2, 'us', 20), (3, 'us', 30),
            (4, 'eu', 40), (5, 'eu', 50),
            (6, 'asia', 60)
        """,
        settings=WRITE_SETTINGS,
    )

    total = _trino_exec(
        cluster,
        f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}"',
    )
    assert total.strip() == "6", f"unfiltered count: {total!r}"

    us = _trino_exec(
        cluster,
        f"SELECT sum(value) FROM \"{NAMESPACE}\".\"{table_name}\" WHERE region = 'us'",
    )
    assert us.strip() == "60", f"sum(value) for region='us': {us!r}"

    eu = _trino_exec(
        cluster,
        f"SELECT count(*) FROM \"{NAMESPACE}\".\"{table_name}\" WHERE region = 'eu'",
    )
    assert eu.strip() == "2", f"count for region='eu': {eu!r}"


def test_multiple_snapshots_and_nulls(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"snap_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, label Nullable(String), value Nullable(Int64))
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )

    node.query(
        f"INSERT INTO {full} VALUES (1, 'a', 100), (2, NULL, 200)",
        settings=WRITE_SETTINGS,
    )
    node.query(
        f"INSERT INTO {full} VALUES (3, 'c', NULL), (4, 'd', 400)",
        settings=WRITE_SETTINGS,
    )

    out = _trino_exec(
        cluster,
        f'SELECT id, label, value FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    assert out == "1\ta\t100\n2\t\t200\n3\tc\t\n4\td\t400\n", f"got {out!r}"

    agg = _trino_exec(
        cluster,
        f"""
        SELECT count(*), count(label), count(value), sum(value)
        FROM "{NAMESPACE}"."{table_name}"
        """,
    )
    assert agg.strip() == "4\t3\t3\t700", f"agg over two snapshots: {agg!r}"


def test_v3_primitive_types(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"types_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (
            id Int32,
            i32 Int32,
            i64 Int64,
            f32 Float32,
            f64 Float64,
            d Date,
            ts DateTime64(6),
            ts_utc DateTime64(6, 'UTC'),
            s String,
            uid UUID,
            opt Nullable(Int64)
        )
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )

    node.query(
        f"""
        INSERT INTO {full} VALUES
            (1, 100,  1000000000000,  1.5,  2.5, '2024-06-15',
             '2024-06-15 10:11:12.000000', '2024-06-15 10:11:12.000000',
             'hello', '550e8400-e29b-41d4-a716-446655440000', 42),
            (2, -100, -1000000000000, -1.5, -2.5, '1970-01-02',
             '1970-01-02 00:00:00.123456', '1970-01-02 00:00:00.123456',
             'world', '00000000-0000-0000-0000-000000000000', NULL)
        """,
        settings=WRITE_SETTINGS,
    )

    out = _trino_exec(
        cluster,
        f"""
        SELECT
            format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                id,
                i32,
                i64,
                f32,
                f64,
                CAST(d AS varchar),
                CAST(ts AS varchar),
                CAST(ts_utc AS varchar),
                s,
                CAST(uid AS varchar),
                COALESCE(CAST(opt AS varchar), 'null')
            )
        FROM "{NAMESPACE}"."{table_name}"
        ORDER BY id
        """,
    )
    expected = (
        "1|100|1000000000000|1.5|2.5|2024-06-15|"
        "2024-06-15 10:11:12.000000|2024-06-15 10:11:12.000000|"
        "hello|550e8400-e29b-41d4-a716-446655440000|42\n"
        "2|-100|-1000000000000|-1.5|-2.5|1970-01-02|"
        "1970-01-02 00:00:00.123456|1970-01-02 00:00:00.123456|"
        "world|00000000-0000-0000-0000-000000000000|null\n"
    )
    assert out == expected, f"primitive round-trip:\nexpected:\n{expected!r}\ngot:\n{out!r}"


def test_v3_time_travel(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"tt_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, value Int64)
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )
    node.query(f"INSERT INTO {full} VALUES (1, 10)", settings=WRITE_SETTINGS)
    node.query(f"INSERT INTO {full} VALUES (2, 20), (3, 30)", settings=WRITE_SETTINGS)
    node.query(f"INSERT INTO {full} VALUES (4, 40)", settings=WRITE_SETTINGS)

    snaps_out = _trino_exec(
        cluster,
        f'SELECT snapshot_id FROM "{NAMESPACE}"."{table_name}$snapshots" ORDER BY committed_at',
    )
    snap_ids = [line for line in snaps_out.strip().splitlines() if line]
    assert len(snap_ids) == 3, f"expected 3 snapshots, got {snap_ids!r}"

    for snap_id, expected_count, expected_sum in zip(
        snap_ids, [1, 3, 4], [10, 60, 100]
    ):
        out = _trino_exec(
            cluster,
            f'SELECT count(*), sum(value) FROM "{NAMESPACE}"."{table_name}" '
            f"FOR VERSION AS OF {snap_id}",
        )
        assert out.strip() == f"{expected_count}\t{expected_sum}", (
            f"FOR VERSION AS OF {snap_id}: expected "
            f"{expected_count}\\t{expected_sum}, got {out!r}"
        )


def test_v3_iceberg_system_tables(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"meta_table_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, payload String)
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 3
        """,
        settings=WRITE_SETTINGS,
    )
    node.query(f"INSERT INTO {full} VALUES (1, 'a')", settings=WRITE_SETTINGS)
    node.query(f"INSERT INTO {full} VALUES (2, 'b')", settings=WRITE_SETTINGS)

    snaps = _trino_exec(
        cluster,
        f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}$snapshots"',
    )
    assert snaps.strip() == "2", f"$snapshots count: {snaps!r}"

    files = _trino_exec(
        cluster,
        f'SELECT count(*), sum(record_count) FROM "{NAMESPACE}"."{table_name}$files"',
    )
    assert files.strip() == "2\t2", f"$files count/sum(record_count): {files!r}"

    history = _trino_exec(
        cluster,
        f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}$history"',
    )
    assert history.strip() == "2", f"$history count: {history!r}"
