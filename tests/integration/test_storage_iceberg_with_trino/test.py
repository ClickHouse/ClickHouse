import json
import logging
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

from .catalog_proxy import catalog_commit_barrier


REST_URL = "http://rest:8181/v1"
NAMESPACE = "default"

MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "ClickHouse_Minio_P@ssw0rd"
WAREHOUSE_BUCKET = "warehouse-rest"

CATALOG_DATABASE = "iceberg_trino_test"

# Transient at startup: the coordinator registers in system.runtime.nodes (so the
# readiness probe passes) before the scheduler's worker node map is populated, so the
# first distributed scans fail with this until scheduling catches up.
TRINO_NO_NODES_ERROR = "No nodes available to run query"


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


def _trino_exec(cluster: ClickHouseCluster, sql: str, retries: int = 20) -> str:
    container = _trino_container_name(cluster)
    last_proc = None
    for attempt in range(retries):
        last_proc = subprocess.run(
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
        if last_proc.returncode == 0:
            return last_proc.stdout
        # Retry only the startup scheduling race; any other error is a real failure.
        if TRINO_NO_NODES_ERROR in last_proc.stderr and attempt < retries - 1:
            time.sleep(1)
            continue
        break
    raise RuntimeError(
        f"Trino query failed (exit {last_proc.returncode}):\n"
        f"  SQL: {sql}\n  stdout: {last_proc.stdout}\n  stderr: {last_proc.stderr}"
    )


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


@pytest.fixture(scope="module")
def pause_trino_commit(started_cluster_with_trino):
    """Forward a second catalog to the same REST service, holding one snapshot commit."""
    cluster = started_cluster_with_trino
    container = _trino_container_name(cluster)
    with catalog_commit_barrier(cluster.iceberg_rest_catalog_port) as (
        proxy_port,
        pause_commit,
    ):
        config_path = "/etc/trino/catalog/commit_barrier.properties"
        try:
            network = json.loads(
                subprocess.check_output(["docker", "inspect", container], text=True)
            )[0]["NetworkSettings"]["Networks"]
            gateway = next(iter(network.values()))["Gateway"]
            config = subprocess.check_output(
                ["docker", "exec", container, "cat", "/etc/trino/catalog/iceberg.properties"],
                text=True,
            ).replace("http://rest:8181", f"http://{gateway}:{proxy_port}")
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    "-u",
                    "root",
                    container,
                    "sh",
                    "-c",
                    f"cat > {config_path}",
                ],
                input=config,
                text=True,
                check=True,
            )
            subprocess.run(["docker", "restart", container], check=True)
            _wait_for_trino_ready(cluster, timeout_seconds=120)
            yield pause_commit
        finally:
            subprocess.run(
                ["docker", "exec", "-u", "root", container, "rm", "-f", config_path],
                check=True,
            )


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


def test_optimize_manifest_trino_field_ids(iceberg_db):
    # Field-id regression: renaming a column keeps its Iceberg field-id but changes the name, so a
    # correct Trino read of the new name after OPTIMIZE ... MANIFEST proves the rewrite kept field-ids.
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"optimize_manifest_fieldid_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    # Manifest compaction is supported only for Iceberg format-version 2 (v1 and v3 are rejected).
    node.query(
        f"""
        CREATE TABLE {full} (id Int32, secret String)
        {_engine_clause(table_name)}
        SETTINGS iceberg_format_version = 2
        """,
        settings=WRITE_SETTINGS,
    )

    # Several separate inserts -> several data manifests to consolidate.
    for i in range(1, 6):
        node.query(
            f"INSERT INTO {full} VALUES ({i}, 'val{i}')",
            settings=WRITE_SETTINGS,
        )

    # Rename the column: the Iceberg field-id is preserved, only the name changes. The data files
    # keep the original field-id, so a correct read of the new name relies on field-id resolution.
    node.query(
        f"ALTER TABLE {full} RENAME COLUMN secret TO revealed",
        settings=WRITE_SETTINGS,
    )

    read_sql = f'SELECT id, revealed FROM "{NAMESPACE}"."{table_name}" ORDER BY id'
    expected = "1\tval1\n2\tval2\n3\tval3\n4\tval4\n5\tval5\n"

    before = _trino_exec(cluster, read_sql)
    assert before == expected, (
        f"Trino read of renamed column before compaction: expected {expected!r}, got {before!r}"
    )

    snaps_before = _trino_exec(
        cluster, f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}$snapshots"'
    ).strip()

    node.query(
        f"OPTIMIZE TABLE {full} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
            "allow_insert_into_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    # Confirm the compaction actually ran (committed a new snapshot), so the read-back below is not a
    # vacuous no-op pass.
    snaps_after = _trino_exec(
        cluster, f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}$snapshots"'
    ).strip()
    assert int(snaps_after) > int(snaps_before), (
        f"OPTIMIZE TABLE ... MANIFEST did not commit a new snapshot "
        f"({snaps_before} -> {snaps_after})"
    )

    # After the manifest rewrite, Trino must still resolve the renamed column by its preserved
    # field-id and return the original data.
    after = _trino_exec(cluster, read_sql)
    assert after == expected, (
        f"Trino read of renamed column after OPTIMIZE MANIFEST: expected {expected!r}, got {after!r}"
    )


def test_drop_partition(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"drop_partition_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, region String, value Int64)
        {_engine_clause(table_name)}
        PARTITION BY identity(region)
        SETTINGS iceberg_format_version = 2
        """,
        settings=WRITE_SETTINGS,
    )

    # Keep target partitions in their own manifests because partial manifest rewriting is
    # covered separately. The non-target partitions may share a manifest.
    node.query(
        f"INSERT INTO {full} VALUES (2, 'eu', 20), (3, 'asia', 30)",
        settings=WRITE_SETTINGS,
    )
    node.query(f"INSERT INTO {full} VALUES (1, 'us', 10)", settings=WRITE_SETTINGS)
    node.query(f"INSERT INTO {full} VALUES (4, 'us', 40)", settings=WRITE_SETTINGS)

    node.query(
        f"ALTER TABLE {full} DROP PARTITION 'us'",
        settings=WRITE_SETTINGS,
    )

    clickhouse_rows = node.query(f"SELECT id, region, value FROM {full} ORDER BY id")
    trino_rows = _trino_exec(
        cluster,
        f'SELECT id, region, value FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    expected = "2\teu\t20\n3\tasia\t30\n"
    assert clickhouse_rows == expected
    assert trino_rows == expected

    snapshots = _trino_exec(
        cluster,
        f'SELECT count(*) FROM "{NAMESPACE}"."{table_name}$snapshots"',
    )
    assert snapshots.strip() == "4"

    files = _trino_exec(
        cluster,
        f'SELECT count(*), sum(record_count) FROM "{NAMESPACE}"."{table_name}$files"',
    )
    assert files.strip() == "2\t2"

    _trino_exec(
        cluster,
        f'INSERT INTO "{NAMESPACE}"."{table_name}" VALUES (5, \'africa\', 50)',
    )

    clickhouse_rows = node.query(f"SELECT id, region, value FROM {full} ORDER BY id")
    trino_rows = _trino_exec(
        cluster,
        f'SELECT id, region, value FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    expected = "2\teu\t20\n3\tasia\t30\n5\tafrica\t50\n"
    assert clickhouse_rows == expected
    assert trino_rows == expected


def _create_trino_partitioned_table(cluster, columns, partitioning):
    name = f"drop_partition_{_get_uuid_str()}"
    table = f'"{NAMESPACE}"."{name}"'
    _trino_exec(cluster, f'CREATE SCHEMA IF NOT EXISTS "{NAMESPACE}"')
    _trino_exec(
        cluster,
        f"CREATE TABLE {table} ({columns}) "
        f"WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['{partitioning}'])",
    )
    return name, table, f"{CATALOG_DATABASE}.`{NAMESPACE}.{name}`"


@pytest.mark.parametrize(
    "partitioning,key_type,keys,drop_key",
    [
        pytest.param("k", "VARCHAR", ("us", "eu", "asia"), "{}", id="identity"),
        pytest.param(
            "bucket(k, 16)",
            "VARCHAR",
            ("apple", "banana", "cherry"),
            "icebergBucket(16, {})",
            id="bucket",
        ),
        pytest.param(
            "truncate(k, 2)",
            "VARCHAR",
            ("us-east", "eu-west", "asia-east"),
            "icebergTruncate(2, {})",
            id="truncate",
        ),
        pytest.param(
            "year(k)",
            "DATE",
            ("2023-03-15", "2024-08-01", "2025-01-10"),
            "icebergYear(toDate({}))",
            id="year",
        ),
        pytest.param(
            "month(k)",
            "DATE",
            ("2025-01-05", "2025-02-10", "2025-03-15"),
            "icebergMonth(toDate({}))",
            id="month",
        ),
        pytest.param(
            "day(k)",
            "DATE",
            ("2025-05-01", "2025-05-02", "2025-05-03"),
            "icebergDay(toDate({}))",
            id="day",
        ),
        pytest.param(
            "hour(k)",
            "TIMESTAMP(6)",
            ("2025-05-19 10:15:00", "2025-05-19 11:30:00", "2025-05-19 12:45:00"),
            "icebergHour(toDateTime64({}, 6, 'UTC'))",
            id="hour",
        ),
    ],
)
def test_drop_partition_partial_manifest(
    iceberg_db, partitioning, key_type, keys, drop_key
):
    cluster = iceberg_db
    node = cluster.instances["node1"]
    name, table, full = _create_trino_partitioned_table(
        cluster, f"id INTEGER, k {key_type}", partitioning
    )
    values = ", ".join(
        f"({row_id}, CAST('{key}' AS {key_type}))" for row_id, key in enumerate(keys, 1)
    )
    _trino_exec(cluster, f"INSERT INTO {table} VALUES {values}")
    assert _trino_exec(
        cluster,
        f'SELECT count(*), sum(added_data_files_count) FROM "{NAMESPACE}"."{name}$manifests"',
    ).strip() == "1\t3"

    key = drop_key.format(f"'{keys[0]}'")
    node.query(f"ALTER TABLE {full} DROP PARTITION tuple({key})", settings=WRITE_SETTINGS)
    assert node.query(f"SELECT id FROM {full} ORDER BY id") == "2\n3\n"
    assert _trino_exec(cluster, f"SELECT id FROM {table} ORDER BY id") == "2\n3\n"

    # Trino can write to the dropped partition after the manifest rewrite.
    _trino_exec(
        cluster, f"INSERT INTO {table} VALUES (4, CAST('{keys[0]}' AS {key_type}))"
    )
    assert node.query(f"SELECT id FROM {full} ORDER BY id") == "2\n3\n4\n"
    assert _trino_exec(cluster, f"SELECT id FROM {table} ORDER BY id") == "2\n3\n4\n"


def test_drop_partition_preserves_manifest_metadata(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]
    keys = ("us", "eu", "asia")
    table_name, trino_table, full = _create_trino_partitioned_table(
        cluster, "id INTEGER, k VARCHAR, value BIGINT", "k"
    )

    def insert(rows):
        values = ", ".join(
            f"({row_id}, '{keys[partition]}', {row_id * 10})" for row_id, partition in rows
        )
        _trino_exec(cluster, f"INSERT INTO {trino_table} VALUES {values}")

    def drop_partition(partition):
        node.query(
            f"ALTER TABLE {full} DROP PARTITION tuple('{keys[partition]}')",
            settings=WRITE_SETTINGS,
        )

    def check_rows(ids):
        expected = "".join(f"{row_id}\t{row_id * 10}\n" for row_id in ids)
        assert node.query(f"SELECT id, amount FROM {full} ORDER BY id") == expected
        assert (
            _trino_exec(cluster, f"SELECT id, amount FROM {trino_table} ORDER BY id")
            == expected
        )

    def entries(predicate):
        rows = _trino_exec(
            cluster,
            f"""SELECT data_file.file_path, snapshot_id, sequence_number, file_sequence_number, data_file
            FROM "{NAMESPACE}"."{table_name}$entries" WHERE {predicate}""",
        )
        return dict(row.split("\t", 1) for row in rows.splitlines())

    def manifests():
        rows = _trino_exec(
            cluster, f'SELECT path, * FROM "{NAMESPACE}"."{table_name}$manifests"'
        )
        return dict(row.split("\t", 1) for row in rows.splitlines())

    def current_snapshot():
        return _trino_exec(
            cluster,
            f'SELECT snapshot_id FROM "{NAMESPACE}"."{table_name}$snapshots" ORDER BY committed_at DESC LIMIT 1',
        ).strip()

    insert((row_id, (row_id - 1) // 2) for row_id in range(1, 7))
    # Verify that Trino actually put all three partitions in one manifest.
    assert _trino_exec(
        cluster,
        f'SELECT count(*), sum(added_data_files_count) FROM "{NAMESPACE}"."{table_name}$manifests"',
    ).strip() == "1\t3"
    initial_snapshot = current_snapshot()

    # Add an unrelated manifest that must be carried forward without rewriting it.
    original_manifests = manifests()
    insert([(9, 2)])
    unrelated_manifests = {
        path: entry for path, entry in manifests().items() if path not in original_manifests
    }
    assert unrelated_manifests

    # Write position deletes in both the target and surviving partitions, then rewrite
    # a manifest whose schema predates a rename. Preserve its bounds and field IDs.
    _trino_exec(cluster, f"DELETE FROM {trino_table} WHERE id IN (1, 3)")
    _trino_exec(cluster, f"ALTER TABLE {trino_table} RENAME COLUMN value TO amount")
    before = entries("status <> 2")
    drop_partition(0)
    check_rows([4, 5, 6, 9])
    after = entries("status <> 2")
    assert len(after) == 4  # Three data files and the surviving position-delete file.
    assert after.items() <= before.items()
    assert unrelated_manifests.items() <= manifests().items()

    # New tombstones keep file metadata and sequence numbers, with this drop's snapshot ID.
    deleted = entries("status = 2")
    drop_snapshot = current_snapshot()
    removed = before.keys() - after.keys()
    assert len(removed) == 2  # The target data file and its position-delete file.
    for path in removed:
        _, lineage_and_file = before[path].split("\t", 1)
        assert deleted[path] == f"{drop_snapshot}\t{lineage_and_file}"

    # Trino must still delete rows and append to surviving and dropped partitions.
    insert([(7, 0), (8, 1)])
    _trino_exec(cluster, f"DELETE FROM {trino_table} WHERE id = 5")
    check_rows([4, 6, 7, 8, 9])

    # Rewriting the shared manifest must not report its old deletions again.
    assert entries("status = 2")
    drop_partition(1)
    check_rows([6, 7, 9])
    assert removed.isdisjoint(entries("status = 2"))
    assert unrelated_manifests.items() <= manifests().items()
    assert _trino_exec(
        cluster, f"SELECT count(*) FROM {trino_table} FOR VERSION AS OF {initial_snapshot}"
    ).strip() == "6"


def _create_drop_conflict_table(cluster, shared_manifest):
    name, table, full = _create_trino_partitioned_table(
        cluster, "id INTEGER, p INTEGER, v INTEGER", "p"
    )
    if shared_manifest:
        _trino_exec(cluster, f"INSERT INTO {table} VALUES (1, 1, 10), (2, 2, 20)")
    else:
        _trino_exec(cluster, f"INSERT INTO {table} VALUES (1, 1, 10)")
        _trino_exec(cluster, f"INSERT INTO {table} VALUES (2, 2, 20)")
    assert _trino_exec(
        cluster, f'SELECT count(*) FROM "{NAMESPACE}"."{name}$manifests"'
    ).strip() == ("1" if shared_manifest else "2")
    return name, table, full


@pytest.mark.parametrize("shared_manifest", [False, True], ids=["whole", "partial"])
def test_trino_partition_delete_manifest_entries(iceberg_db, shared_manifest):
    cluster = iceberg_db
    node = cluster.instances["node1"]
    name, table, full = _create_drop_conflict_table(cluster, shared_manifest)
    _trino_exec(cluster, f"DELETE FROM {table} WHERE p = 1")

    entries = _trino_exec(
        cluster,
        f"SELECT status, data_file.content, count(*), sum(data_file.record_count) "
        f'FROM "{NAMESPACE}"."{name}$entries" GROUP BY 1, 2 ORDER BY 1, 2',
    )
    # An untouched manifest keeps its original `ADDED` entry; a rewrite uses `EXISTING`.
    survivor_status = 0 if shared_manifest else 1
    assert entries == f"{survivor_status}\t0\t1\t1\n2\t0\t1\t1\n"
    assert _trino_exec(
        cluster,
        f'SELECT sum(deleted_data_files_count) FROM "{NAMESPACE}"."{name}$manifests"',
    ).strip() == "1"
    expected = "2\t2\t20\n"
    assert _trino_exec(cluster, f"SELECT id, p, v FROM {table}") == expected
    assert node.query(f"SELECT id, p, v FROM {full}") == expected


@pytest.mark.parametrize("shared_manifest", [False, True], ids=["whole", "partial"])
def test_trino_update_conflicts_with_clickhouse_drop(
    iceberg_db, pause_trino_commit, shared_manifest
):
    cluster = iceberg_db
    node = cluster.instances["node1"]
    name, table, full = _create_drop_conflict_table(cluster, shared_manifest)
    expected = "2\t2\t20\n"

    with ThreadPoolExecutor(max_workers=1) as pool:
        # Hold the catalog commit after Trino has prepared its updated rows and delete files.
        with pause_trino_commit(name) as paused:
            update = pool.submit(
                _trino_exec,
                cluster,
                f"UPDATE commit_barrier.{table} SET v = v + 1 WHERE p = 1",
            )
            assert paused.wait(120), "Trino did not reach its snapshot commit"
            assert not update.done()
            node.query(f"ALTER TABLE {full} DROP PARTITION 1", settings=WRITE_SETTINGS)
            assert node.query(f"SELECT id, p, v FROM {full} ORDER BY id") == expected

        # The retried commit must reject the stale update instead of restoring the partition.
        with pytest.raises(RuntimeError, match="Cannot commit, missing data files"):
            update.result(timeout=120)

    assert _trino_exec(cluster, f"SELECT id, p, v FROM {table} ORDER BY id") == expected
    assert node.query(f"SELECT id, p, v FROM {full} ORDER BY id") == expected


def test_drop_partition_with_evolved_spec_is_rejected(iceberg_db):
    cluster = iceberg_db
    node = cluster.instances["node1"]

    table_name = f"drop_partition_evolved_spec_{_get_uuid_str()}"
    full = f"{CATALOG_DATABASE}.`{NAMESPACE}.{table_name}`"

    node.query(
        f"""
        CREATE TABLE {full} (id Int32, region String, value Int64)
        {_engine_clause(table_name)}
        PARTITION BY identity(region)
        SETTINGS iceberg_format_version = 2
        """,
        settings=WRITE_SETTINGS,
    )
    node.query(
        f"INSERT INTO {full} VALUES (1, 'us', 10), (2, 'eu', 20)",
        settings=WRITE_SETTINGS,
    )

    _trino_exec(
        cluster,
        f"""
        ALTER TABLE "{NAMESPACE}"."{table_name}"
        SET PROPERTIES partitioning = ARRAY['region', 'bucket(id, 4)']
        """,
    )
    _trino_exec(
        cluster,
        f'INSERT INTO "{NAMESPACE}"."{table_name}" VALUES (3, \'asia\', 30)',
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {full} DROP PARTITION 'us'",
        settings=WRITE_SETTINGS,
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert "evolved partition specs" in error, error

    clickhouse_rows = node.query(f"SELECT id, region, value FROM {full} ORDER BY id")
    trino_rows = _trino_exec(
        cluster,
        f'SELECT id, region, value FROM "{NAMESPACE}"."{table_name}" ORDER BY id',
    )
    expected = "1\tus\t10\n2\teu\t20\n3\tasia\t30\n"
    assert clickhouse_rows == expected
    assert trino_rows == expected
