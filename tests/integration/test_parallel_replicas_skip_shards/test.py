import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_skip_unavailable_shards(start_cluster):
    expected = "node1\nnode2\nnode3\n"
    assert (
        node1.query(
            "SELECT hostName() as h FROM clusterAllReplicas('two_shards', system.one) order by h",
            settings={
                "enable_parallel_replicas": 0,
                "skip_unavailable_shards": 1,
            },
        )
        == expected
    )

    assert (
        node1.query(
            "SELECT hostName() as h FROM clusterAllReplicas('two_shards', system.one) order by h",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "skip_unavailable_shards": 1,
                # "async_socket_for_remote" : 0,
                # "async_query_sending_for_remote" : 0,
                # "connections_with_failover_max_tries": 0,
            },
        )
        == expected
    )


def test_error_on_unavailable_shards(start_cluster):
    with pytest.raises(QueryRuntimeException):
        node1.query(
            "SELECT hostName() as h FROM clusterAllReplicas('two_shards', system.one) order by h",
            settings={
                "enable_parallel_replicas": 0,
                "skip_unavailable_shards": 0,
            },
        )

    with pytest.raises(QueryRuntimeException):
        node1.query(
            "SELECT hostName() as h FROM clusterAllReplicas('two_shards', system.one) order by h",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "skip_unavailable_shards": 0,
            },
        )
