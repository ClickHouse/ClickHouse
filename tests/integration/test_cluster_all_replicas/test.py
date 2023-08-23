import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


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
            """SELECT hostName() FROM clusterAllReplicas("one_shard_two_nodes", system.one)"""
        )
        == "node1\nnode2\n"
    )
    assert (
        node1.query(
            """SELECT hostName() FROM cluster("one_shard_two_nodes", system.one)"""
        )
        == "node1\n"
    )
