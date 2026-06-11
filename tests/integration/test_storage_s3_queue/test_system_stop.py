"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on an S3Queue table."""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_queue_common import (
    create_table,
    create_mv,
    generate_random_files,
    generate_random_string,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            main_configs=["configs/zookeeper.xml", "configs/s3queue_log.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


# Each generated file has 10 rows.
ROWS_PER_FILE = 10


def setup_consuming_table(started_cluster, node, table):
    files_path = f"{table}_data"
    create_table(started_cluster, node, table, "unordered", files_path)
    create_mv(node, table, f"{table}_dst")
    return files_path


def wait_dst_count(node, table, expected):
    node.query_with_retry(
        f"SELECT count() FROM {table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )


def assert_dst_count_stable(node, table, expected, seconds=10):
    """The streaming task is expected to be stopped, so the row count must not grow."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(node.query(f"SELECT count() FROM {table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_stop_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # STOP halts polling: files added meanwhile are not processed.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # START resumes polling and the new files are processed.
    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_pause_start_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_pause_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # PAUSE stops further activity: files added meanwhile are not processed.
    node.query(f"SYSTEM PAUSE {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_cancel_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_cancel_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # CANCEL interrupts only the current poll; polling keeps going afterwards.
    node.query(f"SYSTEM CANCEL {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_refresh_consuming(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_refresh_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # REFRESH kicks off a poll out of order; polling continues normally.
    node.query(f"SYSTEM REFRESH {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_refresh_runs_once_while_start_keeps_consuming(started_cluster):
    # REFRESH runs exactly one processing cycle out of order without resuming polling; START resumes
    # continuous polling. With the stream STOPped, a single REFRESH processes exactly the files
    # present at that moment, and files added afterwards stay unprocessed until START — whereas after
    # START every later batch is processed without any further command.
    node = started_cluster.instances["instance"]
    table = f"s3queue_refreshonce_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    node.query(f"SYSTEM STOP {table}")

    # First batch, then one REFRESH: the single cycle processes exactly these files.
    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    node.query(f"SYSTEM REFRESH {table}")
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # Second batch, no further REFRESH: polling is still stopped, so REFRESH having run once does not
    # keep processing — these files stay unprocessed.
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # START resumes continuous polling: the pending files are processed and later batches keep being
    # processed "forever" without any further command.
    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)
    generate_random_files(started_cluster, files_path, count=5, start_ind=10)
    wait_dst_count(node, table, 15 * ROWS_PER_FILE)


def test_stop_aborts_inflight_batch_pause_commits_it(started_cluster):
    # The durable boundary for S3Queue is a file being marked Processed in Keeper. PAUSE lets the
    # in-flight batch reach it (files committed); STOP aborts before it, so the files are left
    # Cancelled (reset, not Failed) and reprocessed on resume. A materialized view that sleeps per
    # row keeps the batch in-flight long enough for the command to arrive mid-processing.
    node = started_cluster.instances["instance"]
    n_files = 10
    for verb in ["PAUSE", "STOP"]:
        table = f"s3queue_inflight_{verb.lower()}_{generate_random_string()}"
        files_path = f"{table}_data"
        # A large commit threshold keeps all files in a single batch (one commit), so STOP aborts the
        # whole in-flight batch rather than just the file being read. A single processing thread keeps
        # the files strictly sequential so the slow materialized view actually stretches the batch
        # out in time (otherwise the files are processed in parallel and the batch finishes before the
        # command arrives).
        create_table(
            started_cluster,
            node,
            table,
            "unordered",
            files_path,
            additional_settings={
                "max_processed_files_before_commit": 1000,
                "processing_threads_num": 1,
            },
        )
        node.query(f"DROP TABLE IF EXISTS {table}_dst")
        node.query(
            f"CREATE TABLE {table}_dst (column1 UInt32, column2 UInt32, column3 UInt32) "
            f"ENGINE = MergeTree ORDER BY column1"
        )
        # `sleepEachRow` slows the per-file blocks (0.1s * 10 rows = 1s/file, under the per-block
        # sleep limit) so the batch stays in-flight for ~n_files seconds.
        node.query(
            f"CREATE MATERIALIZED VIEW {table}_mv TO {table}_dst AS "
            f"SELECT column1, column2, column3 FROM {table} WHERE sleepEachRow(0.1) = 0"
        )

        # Pre-load while halted, then resume so a fresh batch opens over all the files and processes
        # them slowly; the files are not marked Processed until the batch commits.
        node.query(f"SYSTEM STOP {table}")
        generate_random_files(started_cluster, files_path, count=n_files, start_ind=0)
        node.query(f"SYSTEM START {table}")

        time.sleep(3)  # the batch is now in-flight: files being read, none committed yet
        node.query(f"SYSTEM {verb} {table}")

        if verb == "PAUSE":
            # The in-flight batch finishes and commits on its own, without START.
            wait_dst_count(node, table, n_files * ROWS_PER_FILE)
        else:
            # The in-flight batch is aborted before any file is marked Processed, so nothing is
            # visible until START reprocesses the reset files.
            assert_dst_count_stable(node, table, 0, seconds=10)
            node.query(f"SYSTEM START {table}")
            wait_dst_count(node, table, n_files * ROWS_PER_FILE)


def test_system_stop_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_allbg_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    node.query("SYSTEM STOP ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # START ALL BACKGROUND resumes every streaming table.
    node.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_pause_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_pauseall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # PAUSE ALL BACKGROUND halts polling for every streaming table.
    node.query("SYSTEM PAUSE ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    node.query("SYSTEM START ALL BACKGROUND")
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_cancel_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_cancelall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # CANCEL ALL BACKGROUND interrupts the current poll but does not halt polling.
    node.query("SYSTEM CANCEL ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_refresh_all_background(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_refreshall_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # REFRESH ALL BACKGROUND kicks off a poll for every streaming table; polling continues.
    node.query("SYSTEM REFRESH ALL BACKGROUND")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


def test_system_stop_requires_grant(started_cluster):
    node = started_cluster.instances["instance"]
    table = f"s3queue_grant_{generate_random_string()}"
    setup_consuming_table(started_cluster, node, table)
    user = f"user_{table}"
    node.query(f"DROP USER IF EXISTS {user}; CREATE USER {user}")

    # A user without the required SYSTEM privilege cannot control the table.
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in node.query_and_get_error(
            f"SYSTEM {verb} {table}", user=user
        )

    # Once granted, every verb succeeds.
    node.query(f"GRANT SYSTEM ON *.* TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        node.query(f"SYSTEM {verb} {table}", user=user)

    node.query(f"DROP USER {user}")
