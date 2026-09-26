import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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
    with pytest.raises(QueryRuntimeException) as exc:
        node.query("KILL query where query_id = '123';")
    assert "No query to kill" in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("KILL QUERY WHERE user = currentUser()")
    assert "No query to kill" in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        node.query("KILL mutation where is_done = 0;")
    assert "No mutation to kill" in str(exc.value)


def test_kill_not_throw_if_noop(start_cluster):
    node.query("KILL query where query_id = '123' SETTINGS kill_throw_if_noop = 0;")
    node.query(
        "KILL QUERY WHERE user = currentUser() SETTINGS kill_throw_if_noop = 0"
    )

    node.query(
        "KILL mutation where is_done = 0 SETTINGS kill_throw_if_noop = 0;"
    )


def test_kill_not_throw_if_query_exist(start_cluster):
    node.exec_in_container(
        [
            "bash",
            "-c",
            'clickhouse client --query_id "test_kill_not_throw_if_query_exist" '
            '-q "SELECT number, sleepEachRow(1) FROM system.numbers LIMIT 100 '
            'SETTINGS function_sleep_max_microseconds_per_block=300000000" '
            '> /dev/null 2>&1 &',
        ],
        privileged=True,
        user="root",
    )

    process_count_query = (
        "SELECT count(*) FROM system.processes "
        "WHERE query_id = 'test_kill_not_throw_if_query_exist'"
    )
    assert_eq_with_retry(node, process_count_query, "1")

    node.query("KILL query where query_id = 'test_kill_not_throw_if_query_exist' SYNC")

    assert_eq_with_retry(node, process_count_query, "0")
