import threading
import time

import psycopg2
import pytest

from helpers.cluster import ClickHouseCluster

POSTGRESQL_PORT = 5433

SELECT_FROM_NUMBERS = """SELECT toString(number), repeat('x', 100) FROM numbers(20000000)
SETTINGS max_block_size = 20000000"""

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/postgresql.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.wait_for_log_line("PostgreSQL compatibility protocol")
        yield cluster
    finally:
        cluster.shutdown()


def test_kill_query_during_output(started_cluster):
    def execute_query():
        conn = psycopg2.connect(
            host=started_cluster.get_instance_ip("node1"),
            port=POSTGRESQL_PORT,
            user="default",
            password="123",
            dbname="default",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(SELECT_FROM_NUMBERS)
                for _ in cur:
                    pass
        except Exception:
            pass
        finally:
            conn.close()

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Consume a chunk")
    time.sleep(1)

    node1.query(
        "KILL QUERY WHERE user='default' AND query LIKE 'SELECT toString(number)%' SYNC",
        user="default",
        password="123",
    )

    query_thread.join()

    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE user='default' AND query LIKE 'SELECT toString(number)%'",
        user="default",
        password="123",
    )
    assert int(result.strip()) == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")
