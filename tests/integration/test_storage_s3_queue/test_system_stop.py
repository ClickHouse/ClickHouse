"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on an S3Queue table."""

import logging
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
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
    result = node.query_with_retry(
        f"SELECT count() FROM {table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )
    assert int(result) == expected, f"expected {expected} rows in {table}_dst, got {result!r}"


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
        "ENGINE = MergeTree ORDER BY column1"
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
            "ENGINE = MergeTree ORDER BY column1"
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
        "ENGINE = MergeTree ORDER BY column1"
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


def test_cancel_during_insert_dedup_off_no_duplicates(started_cluster):
    # Same in-flight CANCEL as the test above, but with deduplication_v2 = 0. With dedup off the fix
    # attaches no cancel callback, so the running insert is NOT aborted: it finishes and commits, and
    # the CANCEL is honored only at the next batch boundary. So every file still lands exactly once --
    # no duplicates and no data loss -- without relying on deduplication to mask a reprocess.
    node = started_cluster.instances["instance"]
    n_files = 10
    table = f"s3q_cancel_inflight_dedup_off_{generate_random_string()}"
    files_path = f"{table}_data"
    create_table(
        started_cluster,
        node,
        table,
        "unordered",
        files_path,
        additional_settings={
            "max_processed_files_before_commit": 1000,
            "processing_threads_num": 1,
            "deduplication_v2": 0,
        },
    )
    node.query(f"DROP TABLE IF EXISTS {table}_dst")
    node.query(
        f"CREATE TABLE {table}_dst (column1 UInt32, column2 UInt32, column3 UInt32) "
        "ENGINE = MergeTree ORDER BY column1"
    )
    node.query(
        f"CREATE MATERIALIZED VIEW {table}_mv TO {table}_dst AS "
        f"SELECT column1, column2, column3 FROM {table} WHERE sleepEachRow(0.1) = 0"
    )

    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=n_files, start_ind=0)
    node.query(f"SYSTEM START {table}")

    time.sleep(3)  # the batch is now in-flight: files being inserted, none committed yet
    node.query(f"SYSTEM CANCEL {table}")

    # Every file lands exactly once: count reaches n_files*ROWS and never grows (no duplicate, no loss).
    wait_dst_count(node, table, n_files * ROWS_PER_FILE)
    assert_dst_count_stable(node, table, n_files * ROWS_PER_FILE, seconds=8)
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_cancel_during_direct_select_does_not_commit_files(started_cluster):
    # A direct SELECT with commit_on_select=1 marks its files Processed at the end of the read. SYSTEM
    # CANCEL mid-read must abort it via the cancel epoch before that commit, so the files stay unprocessed
    # and are read again. A sleep failpoint parks the read so CANCEL lands while a file is in progress.
    node = started_cluster.instances["instance"]
    table = f"s3queue_direct_cancel_{generate_random_string()}"
    files_path = f"{table}_data"
    rows = 500000
    # commit_on_select makes a direct SELECT consume its files; a single thread keeps the read
    # sequential. No materialized view: a commit_on_select read is rejected when views are attached.
    create_table(
        started_cluster,
        node,
        table,
        "unordered",
        files_path,
        additional_settings={
            "commit_on_select": 1,
            "processing_threads_num": 1,
        },
    )
    # One file large enough to need several reader->pull calls, so the sleep failpoint lands between
    # pulls with the file still in Processing state.
    s3_function = (
        f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/"
        f"{started_cluster.minio_bucket}/{files_path}/{table}.csv', 'minio', '{minio_secret_key}')"
    )
    node.query(
        f"INSERT INTO FUNCTION {s3_function} "
        f"SELECT number AS column1, number AS column2, number AS column3 FROM numbers({rows})"
    )

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_sleep_in_generate")
    select = f"SELECT count() FROM {table} SETTINGS stream_like_engine_allow_direct_select = 1"
    result = {}

    def run_select():
        result["answer"], result["error"] = node.query_and_get_answer_with_error(select)

    reader = threading.Thread(target=run_select)
    try:
        reader.start()
        # Wait until the source is parked in the sleep failpoint (a file Processing with rows already
        # read), so the CANCEL below lands mid-read rather than before or after it.
        deadline = time.time() + 30
        parked = False
        while time.time() < deadline:
            in_progress = int(
                node.query(
                    f"SELECT count() FROM system.s3queue_metadata_cache "
                    f"WHERE zookeeper_path ilike '%{table}%' "
                    f"AND status = 'Processing' AND rows_processed > 0"
                )
            )
            if in_progress >= 1:
                parked = True
                break
            time.sleep(0.1)
        assert parked, "sleep failpoint did not park the direct SELECT mid-read"
        node.query(f"SYSTEM CANCEL {table}")
    finally:
        reader.join(timeout=60)
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_sleep_in_generate")

    # CANCEL must abort the read instead of letting it finish and commit the file as Processed.
    assert result.get("error"), (
        "SYSTEM CANCEL during a direct SELECT must abort it, but the read completed"
    )

    # The file was not committed, so a fresh read sees all of it again (reprocessed).
    reprocessed = 0
    for _ in range(30):
        reprocessed = int(node.query(select))
        if reprocessed == rows:
            break
        time.sleep(1)
    assert reprocessed == rows, (
        f"files read by the aborted SELECT must be reprocessed: got {reprocessed}, expected {rows}"
    )


def test_cancel_during_direct_select_aborts_without_commit_on_select(started_cluster):
    # With commit_on_select=0 (the default) a direct SELECT reads without consuming its files, but a
    # SYSTEM CANCEL mid-read must still abort it.
    node = started_cluster.instances["instance"]
    table = f"s3queue_direct_cancel_nocommit_{generate_random_string()}"
    files_path = f"{table}_data"
    rows = 500000
    create_table(
        started_cluster,
        node,
        table,
        "unordered",
        files_path,
        additional_settings={
            "commit_on_select": 0,
            "processing_threads_num": 1,
        },
    )
    # One file large enough to need several reader->pull calls, so the sleep failpoint lands between
    # pulls with the file still in Processing state.
    s3_function = (
        f"s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/"
        f"{started_cluster.minio_bucket}/{files_path}/{table}.csv', 'minio', '{minio_secret_key}')"
    )
    node.query(
        f"INSERT INTO FUNCTION {s3_function} "
        f"SELECT number AS column1, number AS column2, number AS column3 FROM numbers({rows})"
    )

    node.query("SYSTEM ENABLE FAILPOINT object_storage_queue_sleep_in_generate")
    select = f"SELECT count() FROM {table} SETTINGS stream_like_engine_allow_direct_select = 1"
    result = {}

    def run_select():
        result["answer"], result["error"] = node.query_and_get_answer_with_error(select)

    reader = threading.Thread(target=run_select)
    try:
        reader.start()
        # Wait until the source is parked in the sleep failpoint (a file Processing with rows already
        # read), so the CANCEL below lands mid-read rather than before or after it.
        deadline = time.time() + 30
        parked = False
        while time.time() < deadline:
            in_progress = int(
                node.query(
                    f"SELECT count() FROM system.s3queue_metadata_cache "
                    f"WHERE zookeeper_path ilike '%{table}%' "
                    f"AND status = 'Processing' AND rows_processed > 0"
                )
            )
            if in_progress >= 1:
                parked = True
                break
            time.sleep(0.1)
        assert parked, "sleep failpoint did not park the direct SELECT mid-read"
        node.query(f"SYSTEM CANCEL {table}")
    finally:
        reader.join(timeout=60)
        node.query("SYSTEM DISABLE FAILPOINT object_storage_queue_sleep_in_generate")

    # CANCEL must abort the read even though the read would not have committed anything.
    assert result.get("error"), (
        "SYSTEM CANCEL during a commit_on_select=0 direct SELECT must abort it, but the read completed"
    )

    # The abort must not corrupt the queue state: a fresh read still sees the whole file.
    reread = 0
    for _ in range(30):
        reread = int(node.query(select))
        if reread == rows:
            break
        time.sleep(1)
    assert reread == rows, (
        f"the file must stay fully readable after the aborted SELECT: got {reread}, expected {rows}"
    )


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
        "PAUSE should stop after the current batch's durable boundary, but "
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
        "PAUSE should stop after the current batch's durable boundary, but "
        f"{settled // ROWS_PER_FILE}/{n_files} files were processed"
    )

    node.query(f"SYSTEM START {table}")
    wait_dst_count(node, table, n_files * ROWS_PER_FILE)


def test_stopped_table_releases_hash_ring_slot(started_cluster):
    # Two S3Queue tables on one node act as peer replicas: same keeper_path and same S3 path, with
    # hash-ring filtering on, so files are split between them by hash. After SYSTEM STOP one table must
    # drop out of the active registry, so the other replica takes over the files that hashed to the
    # stopped one. Otherwise those files stay assigned to the stopped (still-registered) replica and are
    # never processed by anyone until SYSTEM START or shutdown.
    node = started_cluster.instances["instance"]
    suffix = generate_random_string()
    keeper_path = f"/clickhouse/test_hashring_{suffix}"
    files_path = f"hashring_{suffix}_data"
    table_a = f"s3q_hr_a_{suffix}"
    table_b = f"s3q_hr_b_{suffix}"
    dst = f"s3q_hr_dst_{suffix}"

    # One shared destination so we can count the rows processed by either replica.
    node.query(f"DROP TABLE IF EXISTS {dst}")
    node.query(
        f"CREATE TABLE {dst} (column1 UInt32, column2 UInt32, column3 UInt32) "
        "ENGINE = MergeTree ORDER BY column1"
    )

    settings = {
        "keeper_path": keeper_path,
        "enable_hash_ring_filtering": 1,
        # Poll fast and never back off, so STOP is observed within a poll.
        "polling_min_timeout_ms": 100,
        "polling_max_timeout_ms": 100,
        "polling_backoff_ms": 0,
    }
    for t in (table_a, table_b):
        create_table(started_cluster, node, t, "unordered", files_path,
                     additional_settings=settings)
        node.query(f"DROP TABLE IF EXISTS {t}_mv")
        node.query(
            f"CREATE MATERIALIZED VIEW {t}_mv TO {dst} AS "
            f"SELECT column1, column2, column3 FROM {t}"
        )

    count_active = (
        f"SELECT count() FROM system.zookeeper WHERE path = '{keeper_path}/registry'"
    )
    count_dst = f"SELECT count() FROM {dst}"

    def wait_value(query, expected, timeout=30):
        deadline = time.time() + timeout
        cur = None
        while time.time() < deadline:
            cur = int(node.query(query))
            if cur == expected:
                return cur
            time.sleep(0.3)
        return cur

    # Both replicas register as active and split the first batch between them.
    n1 = 20
    generate_random_files(started_cluster, files_path, count=n1, start_ind=0)
    assert wait_value(count_active, 2) == 2, "both replicas should be active"
    assert wait_value(count_dst, n1 * ROWS_PER_FILE) == n1 * ROWS_PER_FILE

    # STOP one replica: it must release its active-registry slot (the fix). Without it the node lingers.
    node.query(f"SYSTEM STOP {table_a}")
    assert wait_value(count_active, 1) == 1, (
        "a STOPped replica must release its active-registry slot"
    )

    # The running replica now owns the whole ring, so it drains the next batch in full -- including the
    # files that would have hashed to the stopped replica.
    n2 = 20
    generate_random_files(started_cluster, files_path, count=n2, start_ind=n1)
    total = (n1 + n2) * ROWS_PER_FILE
    assert wait_value(count_dst, total) == total, (
        "the running replica must drain files that hashed to the stopped replica"
    )

    node.query(f"SYSTEM START {table_a}")
    node.query(f"DROP TABLE IF EXISTS {table_a} SYNC")
    node.query(f"DROP TABLE IF EXISTS {table_b} SYNC")
    node.query(f"DROP TABLE IF EXISTS {dst} SYNC")


def test_refresh_survives_unready_dependencies(started_cluster):
    # A REFRESH issued while the dependent view is attached but not ready (its target table
    # detached) must run once the target returns, not be lost: a polling round consumed the
    # one-shot permit and then bailed on the dependency check before any processing.
    node = started_cluster.instances["instance"]
    table = f"s3queue_refresh_unready_{generate_random_string()}"
    files_path = setup_consuming_table(started_cluster, node, table)

    generate_random_files(started_cluster, files_path, count=1, start_ind=0)
    wait_dst_count(node, table, ROWS_PER_FILE)

    node.query(f"SYSTEM STOP {table}")
    generate_random_files(started_cluster, files_path, count=5, start_ind=1)
    assert_dst_count_stable(node, table, ROWS_PER_FILE)

    node.query(f"DETACH TABLE {table}_dst")
    node.query(f"SYSTEM REFRESH {table}")
    time.sleep(2)  # polling rounds wake with the view unready and must keep the permit

    # Once the target table is back, the queued REFRESH must still process the file backlog.
    node.query(f"ATTACH TABLE {table}_dst")
    wait_dst_count(node, table, 6 * ROWS_PER_FILE)
    assert_dst_count_stable(node, table, 6 * ROWS_PER_FILE, seconds=5)
