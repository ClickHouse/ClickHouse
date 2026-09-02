import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote(start_cluster):
    assert (
        node1.query(
            """select hostName() h, tcpPort() p, count() from clusterAllReplicas("two_shards", system.one) group by h, p order by h, p"""
        )
        == "node1\t9000\t1\nnode2\t9000\t1\n"
    )
