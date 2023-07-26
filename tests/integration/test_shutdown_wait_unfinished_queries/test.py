import pytest

import threading
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node_wait_queries = cluster.add_instance(
    "node_wait_queries",
    main_configs=["configs/config_wait.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)
node_kill_queries = cluster.add_instance(
    "node_kill_queries",
    main_configs=["configs/config_kill.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

global result


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def do_long_query(node):
    global result

    result = node.query_and_get_answer_with_error(
        "SELECT sleepEachRow(1) FROM system.numbers LIMIT 10",
        settings={"send_logs_level": "trace"},
    )


def test_shutdown_wait_unfinished_queries(start_cluster):
    global result

    long_query = threading.Thread(target=do_long_query, args=(node_wait_queries,))
    long_query.start()

    time.sleep(1)
    node_wait_queries.stop_clickhouse(kill=False)

    long_query.join()

    assert result[0].count("0") == 10

    long_query = threading.Thread(target=do_long_query, args=(node_kill_queries,))
    long_query.start()

    time.sleep(1)
    node_kill_queries.stop_clickhouse(kill=False)

    long_query.join()
    assert "QUERY_WAS_CANCELLED" in result[1]
