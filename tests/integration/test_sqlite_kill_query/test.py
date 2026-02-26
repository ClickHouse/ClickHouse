import pytest
import uuid
import threading
import time
import sqlite3
import os

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

SQLITE_DB_FILE_NAME = "test.db"

SELECT_FROM_SQLITE_TABLE = """SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM test_sqlite.big_data_table
SETTINGS max_block_size = 10000"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance_path = node1.path
        host_db_path = os.path.join(instance_path, "database", "user_files")
        os.makedirs(host_db_path, exist_ok=True)
        db_file_on_host = os.path.join(host_db_path, SQLITE_DB_FILE_NAME)

        conn = sqlite3.connect(db_file_on_host)
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS big_data_table;")
        cursor.execute(
            f"""
        CREATE TABLE big_data_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            random_int INTEGER,
            random_string TEXT,
            created_at TIMESTAMP
        );
        """
        )
        cursor.execute(
            f"""
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


# Stop clickhouse-client by SIGINT signal that is the same as pressing Ctrl+C
def stop_clickhouse_client():
    client_pid = node1.get_process_pid("clickhouse client")
    node1.exec_in_container(
        ["bash", "-c", f"kill -INT {client_pid}"],
        user="root",
    )


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

    node1.wait_for_log_line("Generate a chuck")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
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
    node1.wait_for_log_line("Generate a chuck")
    time.sleep(1)

    stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")
