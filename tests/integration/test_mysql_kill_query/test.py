import pytest
import uuid
import threading
import logging
import warnings
import time
import pymysql
from helpers.config_cluster import mysql_pass
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    user_configs=["configs/users.xml"],
    with_mysql8=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        mysql_conn = pymysql.connect(
            user="root",
            password=mysql_pass,
            host=cluster.mysql8_ip,
            port=cluster.mysql8_port,
        )
        with mysql_conn.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS test_db")
            cursor.execute("CREATE DATABASE test_db")
            cursor.execute("USE test_db")
        mysql_conn.commit()
        yield mysql_conn, cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def setup_infinite_query(started_cluster):
    mysql_conn, cluster = started_cluster
    with mysql_conn.cursor() as cursor:
        # Create function with infinite sleep (sleeps one year)
        cursor.execute(
            """CREATE FUNCTION infinite_sleep() RETURNS INT
DETERMINISTIC
NO SQL
BEGIN
DO SLEEP(31536000);
RETURN 1;
END"""
        )
        cursor.execute(
            """CREATE VIEW infinite_counter AS
SELECT 1 AS col
WHERE infinite_sleep() = 1;"""
        )
    mysql_conn.commit()
    yield mysql_conn, cluster


@pytest.fixture(scope="module")
def setup_big_data_table(started_cluster):
    mysql_conn, cluster = started_cluster
    with mysql_conn.cursor() as cursor:
        # Create big table
        cursor.execute(
            """CREATE TABLE big_data_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    random_int INT,
    random_string VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB; """
        )

        # Create procedure to insert data
        cursor.execute(
            """CREATE PROCEDURE insert_big_data(IN num_rows INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  START TRANSACTION;
  WHILE i < num_rows DO
    INSERT INTO big_data_table (random_int, random_string)
    VALUES (
      FLOOR(RAND() * 1000000),
      SUBSTRING(UUID(), 1, 10)
    );
    SET i = i + 1;
    IF i % 10000 = 0 THEN
      COMMIT;
      START TRANSACTION;
    END IF;
  END WHILE;
  COMMIT;
END"""
        )

        # Insert test data using the procedure
        cursor.execute("CALL insert_big_data(1000000)")

    mysql_conn.commit()

    yield mysql_conn, cluster


def test_kill_infinite_query(setup_infinite_query):
    mysql_conn, cluster = setup_infinite_query
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT * FROM mysql(
            '{cluster.mysql8_ip}:{cluster.mysql8_port}',
            'test_db',
            'infinite_counter',
            'root',
            '{mysql_pass}')""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Get data from database")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in MySQL server
    with mysql_conn.cursor() as cursor:
        cursor.execute("SHOW PROCESSLIST;")
        processes = cursor.fetchall()
        logging.debug(f"Processes: {processes}")
        assert len(processes) == 0 or "infinite_counter" not in str(processes)

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_kill_query_during_generation(setup_big_data_table):
    mysql_conn, cluster = setup_big_data_table
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM mysql(
    '{cluster.mysql8_ip}:{cluster.mysql8_port}',
    'test_db',
    'big_data_table',
    'root',
    '{mysql_pass}'
)
SETTINGS max_block_size = 10000""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Get data from database")
    node1.wait_for_log_line("Generate a chuck")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in MySQL server
    with mysql_conn.cursor() as cursor:
        cursor.execute("SHOW PROCESSLIST;")
        processes = cursor.fetchall()
        logging.debug(f"Processes: {processes}")
        assert len(processes) == 0 or "big_data_table" not in str(processes)

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_infinite_query(setup_infinite_query):
    _, cluster = setup_infinite_query

    def execute_query():
        query = f"""SELECT * FROM mysql(
        '{cluster.mysql8_ip}:{cluster.mysql8_port}',
        'test_db',
        'infinite_counter',
        'root',
        '{mysql_pass}')"""
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{query}" """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    node1.wait_for_log_line("Get data from database")
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")


def test_cancel_query_during_generation(setup_big_data_table):
    _, cluster = setup_big_data_table

    def execute_query():
        query = f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM mysql(
    '{cluster.mysql8_ip}:{cluster.mysql8_port}',
    'test_db',
    'big_data_table',
    'root',
    '{mysql_pass}'
)
SETTINGS max_block_size = 10000"""
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{query}" """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Get data from database")
    node1.wait_for_log_line("Generate a chuck")
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")
