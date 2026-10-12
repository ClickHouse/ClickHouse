"""A partially processed file may be aborted on shutdown and replayed from offset 0 only when the
insert deduplicates and every dependent target drops the rows that were already inserted.
"""

import logging
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.s3_queue_common import create_mv, create_table, generate_random_string


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml", "configs/small_insert_blocks.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            stay_alive=True,
        )
        cluster.add_instance(
            "instance_deduplicate_insert_disabled",
            user_configs=[
                "configs/users.xml",
                "configs/small_insert_blocks.xml",
                "configs/deduplicate_insert_disable.xml",
            ],
            with_minio=True,
            with_zookeeper=True,
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "instance_name, dst_settings",
    [
        # The destination does not deduplicate: a plain `MergeTree` with the default
        # `non_replicated_deduplication_window = 0`, so `MergeTreeSink` never consults block ids.
        pytest.param("instance", "", id="target_without_dedup_window"),
        # The destination has a deduplication window, but the insert does not deduplicate:
        # `deduplicate_insert = disable` in the profile overrides the `async_insert_deduplicate`
        # the queue sets for its insert.
        pytest.param(
            "instance_deduplicate_insert_disabled",
            "SETTINGS non_replicated_deduplication_window = 100",
            id="deduplicate_insert_disabled",
        ),
    ],
)
def test_shutdown_dedup_on_target_without_dedup_no_duplicates(
    started_cluster, instance_name, dst_settings
):
    """
    `deduplication_v2 = 1`, but the insert into the destination does not deduplicate (see the
    parameters).

    Aborting a partially processed file on shutdown and replaying its batch from scratch on
    restart is only safe when every dependent target drops the rows that were already inserted
    before the abort. With this destination nothing would drop them, so the replay decision
    must follow what the dependent targets actually do, not the table setting alone: the
    source has to read the in-flight file to EOF before exiting, exactly as it does with
    `deduplication_v2 = 0` (`test_5.py::test_shutdown_dedup_off_no_duplicates`).

    Mechanics: several files are consumed in one batch by two processing threads. The
    `object_storage_queue_sleep_in_generate` failpoint parks one source after its first chunk
    while the other source keeps reading the remaining files, whose blocks reach the
    destination and are committed there (the profile turns squashing off so that they are not
    held back). A restart then sets `shutdown_called` while the parked file is still
    `Processing`. Without the fix that file is marked Cancelled, the whole batch - including
    the files whose rows are already committed - is reset and replayed after the restart, and
    those rows are counted twice. With the fix the parked file is drained, the batch commits,
    and the destination ends up with exactly `files * rows_per_file` rows.
    """
    node = started_cluster.instances[instance_name]
    table_name = f"test_shutdown_replay_{generate_random_string()}"
    dst_table_name = f"{table_name}_dst"
    mv_table_name = f"{table_name}_mv"
    keeper_path = f"/clickhouse/test_{table_name}"
    files_path = f"{table_name}_data"

    format = "column1 Int32, column2 String"
    create_table(
        started_cluster,
        node,
        table_name,
        "unordered",
        files_path,
        format=format,
        additional_settings={
            "keeper_path": keeper_path,
            # Two sources: while one is parked in the failpoint, the other keeps the pipeline
            # moving, so blocks are committed downstream during the pause.
            "s3queue_processing_threads_num": 2,
            # One batch for all the files.
            "max_processed_files_before_commit": 100,
            "polling_max_timeout_ms": 100,
            "polling_min_timeout_ms": 100,
            "deduplication_v2": 1,
        },
    )

    # Each file needs several `reader->pull` calls, so the failpoint sleep lands between pulls
    # with the file still in Processing state when shutdown is observed.
    files_to_generate = 4
    rows_per_file = 300000
    for i in range(files_to_generate):
        file_name = f"file_{table_name}_{uuid.uuid4()}_{i}.csv"
        s3_function = (
            f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/"
            f"{started_cluster.minio_bucket}/{files_path}/{file_name}',"
            f" 'minio', '{minio_secret_key}')"
        )
        node.query(
            f"INSERT INTO FUNCTION {s3_function} "
            f"SELECT number, randomString(100) FROM numbers({rows_per_file})"
        )
    expected_rows = files_to_generate * rows_per_file

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_sleep_in_generate")
    try:
        node.query(
            f"CREATE TABLE {dst_table_name} ({format}, _path String) "
            f"ENGINE = MergeTree ORDER BY column1 {dst_settings}"
        )
        create_mv(
            node,
            table_name,
            dst_table_name,
            mv_name=mv_table_name,
            format=format,
            dst_table_exists=True,
        )

        # Wait for the failpoint to park a source mid-file (a file Processing with rows already
        # counted) and for rows to be committed in the destination, so the restart below lands
        # while a file is in flight and some rows of its batch are already downstream - the rows
        # a replay would duplicate.
        deadline = time.time() + 30
        parked = False
        rows_before_restart = 0
        while time.time() < deadline:
            in_progress = int(
                node.query(
                    f"SELECT count() FROM system.s3queue_metadata_cache "
                    f"WHERE zookeeper_path ilike '%{table_name}%' "
                    f"AND status = 'Processing' AND rows_processed > 0"
                )
            )
            rows_before_restart = int(
                node.query(f"SELECT count() FROM {dst_table_name}")
            )
            if in_progress >= 1 and rows_before_restart > 0:
                parked = True
                break
            time.sleep(0.1)
        assert parked, (
            "object_storage_queue_sleep_in_generate failpoint did not park a source with "
            "rows already in the destination within 30s - test cannot exercise the shutdown path."
        )
        assert rows_before_restart < expected_rows, "the batch must still be in flight"
        node.restart_clickhouse()
    finally:
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_sleep_in_generate")

    processed = 0
    for _ in range(120):
        node.query("SYSTEM FLUSH LOGS system.s3queue_log")
        processed = int(
            node.query(
                f"SELECT count() FROM system.s3queue_log "
                f"WHERE table = '{table_name}' AND status = 'Processed'"
            )
        )
        if processed >= files_to_generate:
            break
        time.sleep(1)
    assert (
        processed >= files_to_generate
    ), f"Only {processed}/{files_to_generate} files reached Processed state"

    # No duplicates: the rows committed before the restart must not be inserted again.
    actual_rows = int(node.query(f"SELECT count() FROM {dst_table_name}"))
    assert actual_rows == expected_rows, (
        f"Expected {expected_rows} rows in destination, got {actual_rows} "
        f"(diff: {actual_rows - expected_rows}, {rows_before_restart} rows were "
        f"already there before the restart)"
    )

    node.query(f"DROP TABLE {mv_table_name} SYNC")
    node.query(f"DROP TABLE {table_name} SYNC")
    node.query(f"DROP TABLE {dst_table_name} SYNC")
