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
            """SELECT hostName() FROM clusterAllReplicas("two_shards", system.one)"""
        )
        == "node1\nnode2\n"
    )
    assert (
        node1.query("""SELECT hostName() FROM cluster("two_shards", system.one)""")
        == "node1\n"
    )
