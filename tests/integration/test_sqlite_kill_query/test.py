import pytest
import uuid
import threading
import time
import sqlite3
import os

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

SQLITE_DB_FILE_NAME = "test.db"

SELECT_FROM_SQLITE_TABLE = """SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM test_sqlite.big_data_table
SETTINGS max_block_size = 10000"""

# Filled in by started_cluster: absolute path to the SQLite db file on the host,
# used to take an exclusive lock and force SQLITE_BUSY on the reader.
DB_FILE_ON_HOST = None


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance_path = node1.path
        host_db_path = os.path.join(instance_path, "database", "user_files")
        os.makedirs(host_db_path, exist_ok=True)
        db_file_on_host = os.path.join(host_db_path, SQLITE_DB_FILE_NAME)

        global DB_FILE_ON_HOST
        DB_FILE_ON_HOST = db_file_on_host

        conn = sqlite3.connect(db_file_on_host)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS big_data_table;")
        cursor.execute(
            """
        CREATE TABLE big_data_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            random_int INTEGER,
            random_string TEXT,
            created_at TIMESTAMP
        );
        """
        )
        cursor.execute(
            """
WITH RECURSIVE r(n) AS (
    VALUES(0)
    UNION ALL
    SELECT n+1 FROM r WHERE n < 999999
)
INSERT INTO big_data_table (random_int, random_string, created_at)
SELECT
    (ABS(RANDOM()) >> 16) % 1000000 + 1 random_int,
    printf('%s-%s-%s',
           substr(hex(RANDOM()),1,8),
           substr(hex(RANDOM()),1,4),
           substr(hex(RANDOM()),1,12)) random_string,
    datetime('now',
             printf('-%d seconds', ABS(RANDOM()) % 31536000)) created_at
FROM r;
        """
        )
        node1.query("DROP DATABASE IF EXISTS test_sqlite;")
        node1.query("CREATE DATABASE test_sqlite;")
        node1.query(
            f"""CREATE TABLE test_sqlite.big_data_table
(
    id UInt64,
    random_int Int32,
    random_string String,
    created_at DateTime64(3)
)
ENGINE = SQLite('{SQLITE_DB_FILE_NAME}', 'big_data_table');
    """
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_kill_query(started_cluster):
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            SELECT_FROM_SQLITE_TABLE,
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Generate a chunk")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_query(started_cluster):
    def execute_query():
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{SELECT_FROM_SQLITE_TABLE}" --format Null""",
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    # Use look_behind_lines=0 to only match new log lines, avoiding stale matches
    # from the preceding test_kill_query which also produces "Generate a chunk".
    node1.wait_for_log_line("Generate a chunk", look_behind_lines=0)
    time.sleep(1)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")


# How long to hold the exclusive lock while the read waits on SQLITE_BUSY.
BUSY_WAIT_SECONDS = 3


def test_kill_query_while_sqlite_busy(started_cluster):
    # Take an exclusive lock on the SQLite database from a separate connection so that
    # sqlite3_step in SQLiteSource::generate returns SQLITE_BUSY. Before the fix the
    # retry loop did a bare `continue`, busy-spinning a full CPU core. The fix sleeps
    # briefly and checks isCancelled() between retries, so the read idles at ~0 CPU
    # and stays cancellable.
    #
    # This test asserts BOTH properties:
    #  1) no-busy-spin: the OS CPU time the query burns while blocked on the lock is a
    #     small fraction of the wall-clock time it spends waiting. Without the sleep the
    #     retry loop pins ~100% of a core (CPU ~= wall time); with the sleep it is ~0.
    #     The test would still pass on `master` (which is already cancellable) if it only
    #     checked cancellation, so this CPU bound is what actually protects the fix.
    #  2) cancellable: KILL cancels the read promptly even while the lock is held.

    # Warm the pooled SQLite connection so its schema is cached. Otherwise the first
    # sqlite3_prepare_v2 needs a shared lock to read sqlite_master and fails immediately
    # under the exclusive lock below, before generate() (and the busy-retry loop) runs.
    node1.query("SELECT count() FROM test_sqlite.big_data_table")

    conn = sqlite3.connect(DB_FILE_ON_HOST, isolation_level=None)
    conn.execute("BEGIN EXCLUSIVE")
    conn.execute(
        "INSERT INTO big_data_table (random_int, random_string) VALUES (1, 'x')"
    )
    try:
        query_id = str(uuid.uuid4())

        def execute_query():
            _, error = node1.query_and_get_answer_with_error(
                SELECT_FROM_SQLITE_TABLE,
                query_id=query_id,
            )
            assert "DB::Exception: Query was cancelled" in error

        query_thread = threading.Thread(target=execute_query)
        query_thread.start()

        # Wait until the read is running and blocked on the lock. Poll system.processes
        # rather than the server log: with the fix the read logs "Generate a chunk" only
        # once and then blocks silently in the retry loop, so a log tail would race.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "1",
            retry_count=60,
            sleep_time=0.5,
        )

        # Let the read sit blocked on SQLITE_BUSY for a fixed window. During this window
        # the fixed server idles; a busy-spinning server would burn a full core.
        time.sleep(BUSY_WAIT_SECONDS)

        # The query is stuck retrying on SQLITE_BUSY (lock is still held), so it must
        # not have finished on its own.
        assert (
            int(
                node1.query(
                    f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
                ).strip()
            )
            == 1
        )

        # KILL must cancel it promptly even though the lock is still held.
        node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")
        query_thread.join()

        assert (
            int(
                node1.query(
                    f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
                ).strip()
            )
            == 0
        )
    finally:
        conn.rollback()
        conn.close()

    # Read the CPU the query consumed from query_log. OSCPUVirtualTimeMicroseconds is the
    # OS-level CPU time for the query; while blocked on the lock the fix consumes almost
    # none, whereas the old bare-continue loop consumes ~one core (CPU ~= wall time).
    node1.query("SYSTEM FLUSH LOGS")
    cpu_us, duration_ms = (
        node1.query(
            f"""SELECT
                    ProfileEvents['OSCPUVirtualTimeMicroseconds'],
                    query_duration_ms
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'ExceptionWhileProcessing'
                ORDER BY event_time_microseconds DESC
                LIMIT 1"""
        )
        .strip()
        .split("\t")
    )
    cpu_us = int(cpu_us)
    duration_ms = int(duration_ms)
    # The query waited on the lock for essentially its whole lifetime. If the retry loop
    # busy-spun, cpu_us would be ~= duration_ms * 1000 (one full core). The fix keeps it
    # far below; use half the wall-clock time as a generous, noise-tolerant bound.
    assert duration_ms >= BUSY_WAIT_SECONDS * 1000 // 2
    assert cpu_us < duration_ms * 1000 * 0.5, (
        f"SQLiteSource busy-spun on SQLITE_BUSY: consumed {cpu_us} us CPU over "
        f"{duration_ms} ms wall time"
    )
