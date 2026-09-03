import threading
import time

import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

MYSQL_PORT = 9001

SELECT_FROM_NUMBERS = """SELECT toString(number), repeat('x', 100) FROM numbers(20000000)
SETTINGS max_block_size = 20000000"""

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/mysql.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.wait_for_log_line("MySQL compatibility protocol")
        yield cluster
    finally:
        cluster.shutdown()


def test_kill_query_during_output(started_cluster):
    def execute_query():
        conn = pymysql.connections.Connection(
            host=started_cluster.get_instance_ip("node"),
            port=MYSQL_PORT,
            user="default",
            password="123",
            database="default",
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

    node.wait_for_log_line("Consume a chunk")
    time.sleep(1)

    node.query(
        "KILL QUERY WHERE user='default' AND query LIKE 'SELECT toString(number)%' SYNC",
        user="default",
        password="123",
    )

    query_thread.join()

    result = node.query(
        "SELECT count(*) FROM system.processes WHERE user='default' AND query LIKE 'SELECT toString(number)%'",
        user="default",
        password="123",
    )
    assert int(result.strip()) == 0

    assert node.contains_in_log("QUERY_WAS_CANCELLED")
