import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryTimeoutExceedException, QueryRuntimeException

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
        node.query("SELECT sleep(3)", timeout=1)
    with pytest.raises(QueryRuntimeException):
        node.query("SELECT 1", settings={'max_concurrent_queries_for_user': 1})
    assert node.contains_in_log('Too many simultaneous queries for user')
    assert not node.contains_in_log('Unknown packet')
