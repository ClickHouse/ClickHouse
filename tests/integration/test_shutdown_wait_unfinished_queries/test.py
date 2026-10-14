import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


def do_long_query(node, query_id):
    global result

    result = node.query_and_get_answer_with_error(
        "SELECT sleepEachRow(1) FROM system.numbers LIMIT 10",
        settings={"send_logs_level": "trace"},
        query_id=query_id,
    )


def test_shutdown_wait_unfinished_queries(start_cluster):
    global result

    query_id = uuid.uuid4().hex
    long_query = threading.Thread(
        target=do_long_query,
        args=(
            node_wait_queries,
            query_id,
        ),
    )
    long_query.start()

    assert_eq_with_retry(
        node_wait_queries,
        f"SELECT query_id FROM system.processes WHERE query_id = '{query_id}'",
        query_id,
    )
    node_wait_queries.stop_clickhouse(kill=False)

    long_query.join()

    assert result[0].count("0") == 10

    query_id = uuid.uuid4().hex
    long_query = threading.Thread(
        target=do_long_query,
        args=(
            node_kill_queries,
            query_id,
        ),
    )
    long_query.start()

    assert_eq_with_retry(
        node_kill_queries,
        f"SELECT query_id FROM system.processes WHERE query_id = '{query_id}'",
        query_id,
    )
    node_kill_queries.stop_clickhouse(kill=False)

    long_query.join()
    assert "QUERY_WAS_CANCELLED" in result[1]
