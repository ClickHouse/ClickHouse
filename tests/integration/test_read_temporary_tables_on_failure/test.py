import pytest
from helpers.client import QueryTimeoutExceedException, QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node')


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_different_versions(start_cluster):
    with pytest.raises(QueryTimeoutExceedException):
        node.query("SELECT sleepEachRow(3) FROM numbers(10)", timeout=5)
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT 1", settings={'max_concurrent_queries_for_user': 1})
    assert node.contains_in_log('Too many simultaneous queries for user')
    assert not node.contains_in_log('Unknown packet')
