# coding: utf-8

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()

INCREMENTAL_WRITER_JAR = "/opt/paimon/paimon-incremental-writer.jar"
CLICKHOUSE_WORKDIR = "/var/lib/clickhouse"
USER_FILES_PATH = f"{CLICKHOUSE_WORKDIR}/user_files"

CH_TABLE_NAME = "paimon_inc_read"
CH_TABLE_NAME_WITH_LIMIT = "paimon_inc_read_with_limit"
CH_MV_PAIMON_TABLE = "paimon_mv_source"
CH_MV_MERGETREE_TABLE = "paimon_mv_dest"
CH_MV_NAME = "paimon_refresh_mv"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml", "configs/config.xml"],
    macros={"shard": "s1", "replica": "r1"},
)

cluster.base_cmd.extend(
    ["--file", os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_paimon_incremental_writer.yml")]
)


def wait_for_container(cluster, service_name, timeout=60):
    docker_id = cluster.get_instance_docker_id(service_name)
    container = cluster.get_docker_handle(docker_id)
    start = time.time()
    while time.time() - start < timeout:
        info = container.client.api.inspect_container(container.name)
        if info["State"]["Running"]:
            return
        time.sleep(1)
    raise Exception(f"Container {service_name} did not start in {timeout}s")


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        wait_for_container(cluster, "paimon-incremental-writer")
        yield cluster
    finally:
        cluster.shutdown()


def _wait_until_query_result(
    query: str,
    expected: str,
    *,
    database: str,
    retries: int = 30,
    sleep_seconds: float = 0.5,
):
    last_result = ""
    for _ in range(retries):
        last_result = node.query(query, database=database)
        if last_result == expected:
            return
        time.sleep(sleep_seconds)

    raise AssertionError(
        f"Unexpected result for query: {query}\nExpected: {expected!r}\nActual: {last_result!r}"
    )


def _run_writer(
    container_id: str,
    *,
    warehouse_uri: str,
    start_id: int,
    rows_per_commit: int,
    commit_times: int,
) -> None:
    writer_cmd = (
        f"java -jar {INCREMENTAL_WRITER_JAR} "
        f'"{warehouse_uri}" "test" "test_table" "{start_id}" "{rows_per_commit}" "{commit_times}"'
    )
    run_and_check(
        [f"docker exec {container_id} bash -c '{writer_cmd}'"],
        shell=True,
    )


def _create_clickhouse_table_for_paimon_incremental_read(
    table_name: str, table_path: str, refresh_interval_sec: int = 1
):
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        "CREATE TABLE {table_name} "
        "ENGINE = PaimonLocal('{table_path}') "
        "SETTINGS "
        "paimon_incremental_read = 1, "
        "paimon_keeper_path = '/clickhouse/tables/{{uuid}}', "
        "paimon_replica_name = '{{replica}}', "
        "paimon_metadata_refresh_interval_sec = {refresh_interval_sec}".format(
            table_name=table_name,
            table_path=table_path,
            refresh_interval_sec=refresh_interval_sec,
        ),
        settings={"allow_experimental_paimon_storage_engine": 1},
    )


def _clean_warehouse(container_id: str, warehouse_dir: str):
    run_and_check(
        [f'docker exec {container_id} bash -c "rm -rf {warehouse_dir}"'],
        shell=True,
    )


def test_paimon_incremental_read_via_paimon_table_engine(started_cluster):
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_inc"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    _clean_warehouse(writer_container_id, warehouse_dir)

    # Warm-up commit: ensure there is at least one parquet file so schema can be inferred.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME, table_path)

    # Consume warm-up snapshot and reset incremental state baseline.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # First snapshot: 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Second snapshot: another 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME}",
        "0\n",
        database="default",
    )

    # Targeted snapshot reads are deterministic and do not advance stream state.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=2",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME} SETTINGS paimon_target_snapshot_id=2",
        "10\n",
        database="default",
    )

    # max_consume_snapshots limit: consume at most 2 snapshots per query.
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    _clean_warehouse(writer_container_id, warehouse_dir)

    # Recreate clean Paimon table with one warm-up snapshot for schema inference.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
    _create_clickhouse_table_for_paimon_incremental_read(CH_TABLE_NAME_WITH_LIMIT, table_path)

    # Consume warm-up snapshot before testing max_consume_snapshots behavior.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT}",
        "0\n",
        database="default",
    )

    # Produce 3 snapshots, each snapshot contains 10 rows.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=3)
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "20\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "10\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_TABLE_NAME_WITH_LIMIT} SETTINGS max_consume_snapshots=2",
        "0\n",
        database="default",
    )

    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_TABLE_NAME_WITH_LIMIT} SYNC;")


def test_paimon_to_mergetree_via_refresh_mv(started_cluster):
    """
    Validate the end-to-end pipeline:
      Paimon (incremental read) -> Refreshable MV (APPEND) -> MergeTree

    The refreshable MV periodically selects from the Paimon source table
    (which returns only new data each time) and appends to a MergeTree
    destination table.

    Prerequisites:
      - The Paimon source table must have paimon_metadata_refresh_interval_sec
        enabled so that new snapshots are picked up automatically between
        MV refresh cycles.
    """
    MV_REFRESH_INTERVAL_SEC = 10
    SLEEP_AFTER_WRITE_SEC = MV_REFRESH_INTERVAL_SEC + 5

    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    warehouse_name = "warehouse_mv"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    _clean_warehouse(writer_container_id, warehouse_dir)

    # Warm-up commit: create initial Paimon snapshot so schema can be inferred.
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

    # Create Paimon source table with incremental read enabled.
    _create_clickhouse_table_for_paimon_incremental_read(CH_MV_PAIMON_TABLE, table_path)

    # Consume warm-up snapshot to establish incremental read baseline.
    _wait_until_query_result(
        f"SELECT count() FROM {CH_MV_PAIMON_TABLE}",
        "1\n",
        database="default",
    )
    _wait_until_query_result(
        f"SELECT count() FROM {CH_MV_PAIMON_TABLE}",
        "0\n",
        database="default",
    )

    # Create MergeTree destination table with same schema as Paimon table.
    node.query(f"DROP TABLE IF EXISTS {CH_MV_MERGETREE_TABLE} SYNC;")
    node.query(
        "CREATE TABLE {dest} AS {src} ENGINE = MergeTree() ORDER BY tuple()".format(
            dest=CH_MV_MERGETREE_TABLE, src=CH_MV_PAIMON_TABLE
        )
    )

    # Create refreshable MV in APPEND mode.
    node.query(f"DROP VIEW IF EXISTS {CH_MV_NAME} SYNC;")
    node.query(
        "CREATE MATERIALIZED VIEW {mv} "
        "REFRESH EVERY {interval} SECOND "
        "APPEND "
        "TO {dest} "
        "AS SELECT * FROM {src}".format(
            mv=CH_MV_NAME,
            interval=MV_REFRESH_INTERVAL_SEC,
            dest=CH_MV_MERGETREE_TABLE,
            src=CH_MV_PAIMON_TABLE,
        )
    )

    # --- First batch: write 10 rows to Paimon ---
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    time.sleep(SLEEP_AFTER_WRITE_SEC)

    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "10\n", f"Expected 10 rows after first refresh, got {result}"

    # --- Second batch: write another 10 rows to Paimon ---
    _run_writer(writer_container_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)

    time.sleep(SLEEP_AFTER_WRITE_SEC)

    # MergeTree should accumulate to 20 rows total (APPEND mode).
    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "20\n", f"Expected 20 rows after second refresh, got {result}"

    # Cleanup: stop MV first to prevent background refresh from blocking DDL.
    node.query(f"SYSTEM STOP VIEW {CH_MV_NAME};")
    node.query(f"DROP VIEW IF EXISTS {CH_MV_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_MERGETREE_TABLE} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_PAIMON_TABLE} SYNC;")
