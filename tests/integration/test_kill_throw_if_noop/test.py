import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")



@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_kill_throw_if_noop(start_cluster):

    # serverError 750
    with pytest.raises(QueryRuntimeException) as exc:
        node.query(
            "KILL query where query_id = '123';"
        )
    assert "No query to kill" in str(exc.value)

    # serverError 750
    with pytest.raises(QueryRuntimeException) as exc:
        node.query(
            "KILL mutation where is_done = 0;"
        )
    assert "No query to kill" in str(exc.value)


def test_kill_not_throw_if_noop(start_cluster):
    node.query(
        "KILL query where query_id = '123' SETTINGS kill_throw_if_noop = 0;"
    )

    node.query(
        "KILL mutation where is_done = 0 SETTINGS kill_throw_if_noop = 0;"
    )

def test_kill_not_throw_if_query_exist(start_cluster):
    node.exec_in_container(
        ["bash", "-c", 'clickhouse client --query_id "test_kill_not_throw_if_query_exist" -q "SELECT number, sleepEachRow(1) FROM system.numbers LIMIT 100 settings function_sleep_max_microseconds_per_block=300000000" &'],
        privileged=True,
        user="root",
    )

    assert node.query("SELECT count(*) FROM system.processes where query_id='test_kill_not_throw_if_query_exist'") == "1\n"

    assert "" in node.query(
        "KILL query where query_id = 'test_kill_not_throw_if_query_exist' SYNC"
    )

    assert node.query("SELECT count(*) FROM system.processes where query_id='test_kill_not_throw_if_query_exist'") == "0\n"
