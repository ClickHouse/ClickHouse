import psycopg2
import pytest
import uuid
import threading
import time

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import get_postgres_conn

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn(cluster.postgres_ip, cluster.postgres_port)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS postgres_database")
        cursor.execute("CREATE DATABASE postgres_database")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def setup_infinite_query(started_cluster):
    # Connect to postgres_database database
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE OR REPLACE FUNCTION generate_infinite_sequence(start_from INT DEFAULT 1)
RETURNS SETOF INT AS $$
DECLARE
    counter INT := start_from;
BEGIN
    LOOP
        RETURN NEXT counter;
        counter := counter + 1;
        PERFORM pg_sleep(0.01);
    END LOOP;
END;
$$ LANGUAGE plpgsql;"""
    )
    cursor.execute(
        """CREATE OR REPLACE VIEW infinite_counter AS
SELECT generate_infinite_sequence() as counter;"""
    )

    postgres_host_with__port = (
        f"{started_cluster.postgres_ip}:{started_cluster.postgres_port}"
    )
    yield cursor, postgres_host_with__port
    # Cleanup
    cursor.close()
    conn.close()


@pytest.fixture(scope="module")
def setup_big_data_table(started_cluster):
    # Connect to postgres_database database
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE big_data_table (
    id SERIAL PRIMARY KEY,
    random_int INT,
    random_string VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )
    cursor.execute(
        """INSERT INTO big_data_table (random_int, random_string)
SELECT
    floor(random() * 1000000)::INT as random_int,
    substring(md5(random()::text || clock_timestamp()::text) from 1 for 50) as random_string
FROM generate_series(1, 1000000);
        """
    )
    postgres_host_with__port = (
        f"{started_cluster.postgres_ip}:{started_cluster.postgres_port}"
    )
    yield cursor, postgres_host_with__port
    # Cleanup
    cursor.close()
    conn.close()


def test_kill_infinite_query(setup_infinite_query):
    cursor, postgres_host_with__port = setup_infinite_query
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT * FROM postgresql(
        '{postgres_host_with__port}',
        'postgres_database',
        'infinite_counter',
        'postgres',
        'ClickHouse_PostgreSQL_P@ssw0rd')""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Stream data from database")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in PostgreSQL server
    cursor.execute(
        """SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
and query = 'COPY (SELECT "counter" FROM "infinite_counter") TO STDOUT';
        """
    )
    assert cursor.fetchall()[0][0] == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_kill_query_during_generation(setup_big_data_table):
    cursor, postgres_host_with__port = setup_big_data_table
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM postgresql(
    '{postgres_host_with__port}',
    'postgres_database',
    'big_data_table',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd'
)
SETTINGS max_block_size = 10000""",
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Generate a chuck from stream")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    # Verify that query was successfully cancelled in PostgreSQL server
    cursor.execute(
        """SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
and query = 'COPY (SELECT "counter" FROM "infinite_counter") TO STDOUT';
        """
    )
    assert cursor.fetchall()[0][0] == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_infinite_query(setup_infinite_query):
    _, postgres_host_with__port = setup_infinite_query

    def execute_query():
        query = f"""SELECT * FROM postgresql(
        '{postgres_host_with__port}',
        'postgres_database',
        'infinite_counter',
        'postgres',
        'ClickHouse_PostgreSQL_P@ssw0rd')"""
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{query}" """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    node1.wait_for_log_line(
        'ReadFromPostgreSQL: Query: SELECT "counter" FROM "infinite_counter"'
    )
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")


def test_cancel_query_during_generation(setup_big_data_table):
    _, postgres_host_with__port = setup_big_data_table

    def execute_query():
        query = f"""SELECT sleepEachRow(0.0001), id, random_int, random_string
FROM postgresql(
    '{postgres_host_with__port}',
    'postgres_database',
    'big_data_table',
    'postgres',
    'ClickHouse_PostgreSQL_P@ssw0rd'
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
    node1.wait_for_log_line("Generate a chuck from stream")
    time.sleep(2)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")
