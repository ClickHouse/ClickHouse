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
            with_azurite=True,
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


def assert_dst_count_stable(node, table, expected, seconds=5):
    """The consumer is expected to be stopped, so the row count must not grow.
    Still-running consumer polls every kafka_flush_interval_ms (500ms here)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(node.query(f"SELECT count() FROM {table}_dst")) == expected
        time.sleep(1)


def wait_count_stabilizes(node, table, checks=8, interval=0.5, timeout=60):
    """Poll `SELECT count()` until it stays unchanged for `checks` consecutive reads, then return it.
    A still-draining consumer keeps the value moving, so this returns only once processing has
    actually settled — whether that is after a single batch (fix) or after the whole backlog (bug)."""
    deadline = time.time() + timeout
    last = -1
    stable = 0
    while time.time() < deadline:
        cur = int(node.query(f"SELECT count() FROM {table}_dst"))
        if cur == last:
            stable += 1
            if stable >= checks:
                return cur
        else:
            stable = 1
            last = cur
        time.sleep(interval)
    return last


def setup_slow_consuming_table(started_cluster, node, table, engine_name="S3Queue"):
    """One file per commit + a single thread + a materialized view that sleeps per row, so the
    consumer drains a backlog one durable boundary (one committed file) at a time, and slowly enough
    that a SYSTEM command can land between two batch commits. The durable boundary for S3Queue is a
    file being marked Processed in Keeper."""
    files_path = f"{table}_data"
    create_table(
        started_cluster,
        node,
        table,
        "unordered",
        files_path,
        engine_name=engine_name,
        additional_settings={
            "max_processed_files_before_commit": 1,
            "processing_threads_num": 1,
        },
    )
    node.query(f"DROP TABLE IF EXISTS {table}_dst")
    node.query(
        f"CREATE TABLE {table}_dst (column1 UInt32, column2 UInt32, column3 UInt32) "
        f"ENGINE = MergeTree ORDER BY column1"
    )
    # `sleepEachRow(0.1)` * 10 rows/file = ~1s per committed file (under the per-block sleep limit),
    # which stretches the drain out in time so a command can land mid-drain.
    node.query(
        f"CREATE MATERIALIZED VIEW {table}_mv TO {table}_dst AS "
        f"SELECT column1, column2, column3 FROM {table} WHERE sleepEachRow(0.1) = 0"
    )
    return files_path


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


def test_azure_queue_system_stop_start(started_cluster):
    # `AzureQueue` and `S3Queue` are the very same `StorageObjectStorageQueue`; only the object
    # storage backend differs. This test drives that shared path through the `AzureQueue` engine to
    # prove the engine-name dispatch wires up for it too.
    node = started_cluster.instances["instance"]
    table = f"azurequeue_stop_{generate_random_string()}"
    files_path = f"{table}_data"
    create_table(
        started_cluster, node, table, "unordered", files_path, engine_name="AzureQueue"
    )
    create_mv(node, table, f"{table}_dst")

    generate_random_files(
        started_cluster, files_path, count=5, start_ind=0, storage="azure"
    )
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # STOP halts polling: files added meanwhile are not processed.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(
        started_cluster, files_path, count=5, start_ind=5, storage="azure"
    )
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
    #
    # Unlike the message-queue engines, S3Queue reads and inserts in one fused pipeline, so STOP/CANCEL
    # interrupts the running insert and reprocesses the files on resume (like a crash mid-batch). A STOP
    # landing during the insert can therefore duplicate already-inserted rows; that is accepted here.
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


def test_cancel_during_insert_does_not_duplicate(started_cluster):
    # CANCEL companion to test_stop_aborts_inflight_batch_pause_commits_it. For S3Queue the abort
    # cancels the running insert pipeline and resets the in-flight files; CANCEL keeps polling, so they
    # are reprocessed on their own. The single-batch commit is atomic, so every file lands exactly once.
    node = started_cluster.instances["instance"]
    n_files = 10
    table = f"s3queue_cancel_insert_{generate_random_string()}"
    files_path = f"{table}_data"
    # One commit for the whole batch (threshold 1000) on a single thread, with a per-row-sleeping view,
    # so the batch stays in-flight long enough for CANCEL to land before it commits.
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
    node.query(
        f"CREATE MATERIALIZED VIEW {table}_mv TO {table}_dst AS "
        f"SELECT column1, column2, column3 FROM {table} WHERE sleepEachRow(0.1) = 0"
    )

    # Pre-load while halted, then resume so a fresh batch opens over all the files and processes them
    # slowly; nothing is marked Processed until the batch commits.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=n_files, start_ind=0)
    node.query(f"SYSTEM START {table}")

    # CANCEL the in-flight batch; it keeps polling, so the reset files are reprocessed without START.
    time.sleep(3)
    node.query(f"SYSTEM CANCEL {table}")

    # Every file lands exactly once: count reaches n_files*ROWS and never grows (no duplicate, no loss).
    wait_dst_count(node, table, n_files * ROWS_PER_FILE)
    assert_dst_count_stable(node, table, n_files * ROWS_PER_FILE, seconds=8)


def test_stopped_state_does_not_persist_across_restart(started_cluster):
    # The STOP state lives in in-memory action locks, not in on-disk metadata, so it does not survive
    # a server restart: a STOPped table resumes consuming on its own after the node restarts, with no
    # explicit START. This pins the documented contract that none of these states persist across a
    # restart. The streaming control is shared machinery, so one engine is enough to cover it.
    node = started_cluster.instances["instance"]
    table = f"s3queue_restart_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=5, start_ind=0)
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)

    # STOP really halts polling: files added now stay unprocessed.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=5)
    assert_dst_count_stable(node, table, 5 * ROWS_PER_FILE)

    # The restart drops the in-memory stop; the table comes back consuming and drains the files that
    # were left pending, without any START. (Already-processed files stay Processed via Keeper.)
    node.restart_clickhouse()
    wait_dst_count(node, table, 10 * ROWS_PER_FILE)


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

    # SYSTEM VIEWS (the privilege behind the refreshable-view path) is deliberately not enough:
    # streaming engines are guarded by SYSTEM STREAMING ENGINES specifically.
    node.query(f"GRANT SYSTEM VIEWS ON default.{table} TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in node.query_and_get_error(
            f"SYSTEM {verb} {table}", user=user
        )

    # SYSTEM STREAMING ENGINES on the table is exactly the required privilege; every verb now succeeds.
    node.query(f"GRANT SYSTEM STREAMING ENGINES ON default.{table} TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        node.query(f"SYSTEM {verb} {table}", user=user)

    node.query(f"DROP USER {user}")


def test_pause_stops_after_current_batch(started_cluster):
    # A single `streamToViews()` call drains the file iterator one batch at a time, committing each
    # batch (its durable boundary). PAUSE blocks future cycles but lets the in-flight one finish, so a
    # PAUSE arriving mid-drain must stop processing at the *next* durable boundary — not let the call
    # run on through every remaining batch. Before the fix the inner loop only watched the cancel epoch
    # (which PAUSE does not advance), so a mid-drain PAUSE was ignored and the whole backlog drained.
    node = started_cluster.instances["instance"]
    table = f"s3queue_pausebatch_{generate_random_string()}"
    files_path = setup_slow_consuming_table(started_cluster, node, table)

    n_files = 10
    # Pre-load the whole backlog while stopped, then START so a single `streamToViews()` call opens
    # over all the files and drains them slowly, one committed file at a time.
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=n_files, start_ind=0)
    node.query(f"SYSTEM START {table}")

    # Wait until the drain is demonstrably in progress (a couple of files committed), then PAUSE
    # mid-drain with most of the backlog still pending.
    wait_dst_count(node, table, 2 * ROWS_PER_FILE)
    node.query(f"SYSTEM PAUSE {table}")

    # The in-flight batch reaches its durable boundary and consumption stops: the count settles well
    # below the full backlog instead of draining every remaining file.
    settled = wait_count_stabilizes(node, table)
    assert 0 < settled < n_files * ROWS_PER_FILE, (
        f"PAUSE should stop after the current batch's durable boundary, but "
        f"{settled // ROWS_PER_FILE}/{n_files} files were processed"
    )

    # The files left pending are genuinely unprocessed: START drains the rest.
    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, n_files * ROWS_PER_FILE)


def test_refresh_while_stopped_processes_exactly_one_batch(started_cluster):
    # REFRESH grants exactly one out-of-order cycle even while stopped. For S3Queue a cycle's durable
    # boundary is one committed batch (`max_processed_files_before_commit` files); here that is one
    # file. Before the fix a single REFRESH drained the whole backlog, because the inner loop only
    # watched the cancel epoch and REFRESH does not advance it. With the fix one REFRESH processes
    # exactly one batch and then re-blocks.
    node = started_cluster.instances["instance"]
    table = f"s3queue_refreshonebatch_{generate_random_string()}"
    files_path = setup_slow_consuming_table(started_cluster, node, table)

    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=0)

    # One REFRESH -> exactly one file processed; the remaining four stay pending while stopped.
    node.query(f"SYSTEM REFRESH {table}")
    wait_dst_count(node, table, 1 * ROWS_PER_FILE)
    assert_dst_count_stable(node, table, 1 * ROWS_PER_FILE, seconds=6)

    # A second REFRESH -> exactly one more file.
    node.query(f"SYSTEM REFRESH {table}")
    wait_dst_count(node, table, 2 * ROWS_PER_FILE)
    assert_dst_count_stable(node, table, 2 * ROWS_PER_FILE, seconds=6)

    # START resumes continuous polling and drains the rest.
    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, 5 * ROWS_PER_FILE)


def test_azure_queue_pause_stops_after_current_batch(started_cluster):
    # `AzureQueue` and `S3Queue` share the same `StorageObjectStorageQueue` streaming loop, so the
    # PAUSE-between-batches fix covers both; this drives the same scenario through `AzureQueue`.
    node = started_cluster.instances["instance"]
    table = f"azurequeue_pausebatch_{generate_random_string()}"
    files_path = setup_slow_consuming_table(
        started_cluster, node, table, engine_name="AzureQueue"
    )

    n_files = 10
    node.query(f"SYSTEM STOP {table}")
    generate_random_files(
        started_cluster, files_path, count=n_files, start_ind=0, storage="azure"
    )
    node.query(f"SYSTEM START {table}")

    wait_dst_count(node, table, 2 * ROWS_PER_FILE)
    node.query(f"SYSTEM PAUSE {table}")

    settled = wait_count_stabilizes(node, table)
    assert 0 < settled < n_files * ROWS_PER_FILE, (
        f"PAUSE should stop after the current batch's durable boundary, but "
        f"{settled // ROWS_PER_FILE}/{n_files} files were processed"
    )

    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, n_files * ROWS_PER_FILE)
