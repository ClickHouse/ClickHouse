# coding: utf-8

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, run_and_check


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

INCREMENTAL_WRITER_LOCAL_DIR = os.path.join(SCRIPT_DIR, "paimon-incremental-data")
INCREMENTAL_WRITER_REMOTE_DIR = "/root/paimon-incremental-data"
INCREMENTAL_WRITER_JAR = (
    "/root/paimon-incremental-data/target/paimon-incremental-writer-1.1.1.jar"
)
path_to_userfiles_from_defaut_config = "user_files"
CLICKHOUSE_WORKDIR = "/var/lib/clickhouse"
USER_FILES_PATH = f"{CLICKHOUSE_WORKDIR}/{path_to_userfiles_from_defaut_config}"

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


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


def _copy_directory_to_container(instance_id: str, local_dir: str, remote_dir: str):
    if not os.path.isdir(local_dir):
        raise RuntimeError(f"Directory does not exist: {local_dir}")

    run_and_check(
        [
            "docker cp {local} {cont_id}:{remote}".format(
                local=local_dir, cont_id=instance_id, remote=remote_dir
            )
        ],
        shell=True,
    )


def _prepare_incremental_writer(instance_id: str):
    _copy_directory_to_container(
        instance_id, INCREMENTAL_WRITER_LOCAL_DIR, INCREMENTAL_WRITER_REMOTE_DIR
    )

    # If chunks are provided, assemble fat jar in the container.
    run_and_check(
        [
            "docker exec {cont_id} bash -lc \"cd {remote} && "
            "if ls chunk_* >/dev/null 2>&1; then mkdir -p target && cat chunk_* > target/paimon-incremental-writer-1.1.1.jar; fi\"".format(
                cont_id=instance_id, remote=INCREMENTAL_WRITER_REMOTE_DIR
            )
        ],
        shell=True,
    )

    # Build output from local project is expected to be pre-generated.
    run_and_check(
        [
            "docker exec {cont_id} bash -lc 'test -f {jar}'".format(
                cont_id=instance_id, jar=INCREMENTAL_WRITER_JAR
            )
        ],
        shell=True,
    )


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
    instance_id: str,
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
        [
            "docker exec {cont_id} bash -lc '{cmd}'".format(
                cont_id=instance_id, cmd=writer_cmd
            )
        ],
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


def _clean_warehouse(instance_id: str, warehouse_dir: str):
    run_and_check(
        [
            "docker exec {cont_id} bash -lc \"rm -rf {warehouse}\"".format(
                cont_id=instance_id, warehouse=warehouse_dir
            )
        ],
        shell=True,
    )


def test_paimon_incremental_read_via_paimon_table_engine(started_cluster):
    instance_id = cluster.get_instance_docker_id("node")
    _prepare_incremental_writer(instance_id)

    warehouse_name = "warehouse_inc"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    # Clean warehouse for idempotent re-runs.
    _clean_warehouse(instance_id, warehouse_dir)

    # Warm-up commit: ensure there is at least one parquet file so schema can be inferred.
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

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
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)
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
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)
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
    _clean_warehouse(instance_id, warehouse_dir)

    # Recreate clean Paimon table with one warm-up snapshot for schema inference.
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)
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
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=3)
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
      Paimon (incremental read) → Refreshable MV (APPEND) → MergeTree

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

    instance_id = cluster.get_instance_docker_id("node")
    _prepare_incremental_writer(instance_id)

    warehouse_name = "warehouse_mv"
    warehouse_uri = f"file://{USER_FILES_PATH}/{warehouse_name}/"
    warehouse_dir = f"{USER_FILES_PATH}/{warehouse_name}"
    table_path = f"{USER_FILES_PATH}/{warehouse_name}/test.db/test_table"

    # Clean warehouse for idempotent re-runs.
    _clean_warehouse(instance_id, warehouse_dir)

    # Warm-up commit: create initial Paimon snapshot so schema can be inferred.
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=0, rows_per_commit=1, commit_times=1)

    # Create Paimon source table with incremental read enabled.
    # paimon_metadata_refresh_interval_sec = 1 ensures the Paimon engine
    # detects new snapshots within 1 s — well before the MV refresh cycle.
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

    # Create refreshable MV in APPEND mode with a 10 s refresh cycle.
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
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=1, rows_per_commit=10, commit_times=1)

    # Sleep to wait for the next MV refresh cycle to pick up the new data.
    # Timeline: writer commits → Paimon metadata refreshes within 1 s →
    # MV fires within ≤10 s → data lands in MergeTree.
    time.sleep(SLEEP_AFTER_WRITE_SEC)

    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "10\n", f"Expected 10 rows after first refresh, got {result}"

    # --- Second batch: write another 10 rows to Paimon ---
    _run_writer(instance_id, warehouse_uri=warehouse_uri, start_id=11, rows_per_commit=10, commit_times=1)

    time.sleep(SLEEP_AFTER_WRITE_SEC)

    # MergeTree should accumulate to 20 rows total (APPEND mode).
    result = node.query(f"SELECT count() FROM {CH_MV_MERGETREE_TABLE}")
    assert result == "20\n", f"Expected 20 rows after second refresh, got {result}"

    # Cleanup: stop MV first to prevent background refresh from blocking DDL.
    node.query(f"SYSTEM STOP VIEW {CH_MV_NAME};")
    node.query(f"DROP VIEW IF EXISTS {CH_MV_NAME} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_MERGETREE_TABLE} SYNC;")
    node.query(f"DROP TABLE IF EXISTS {CH_MV_PAIMON_TABLE} SYNC;")
