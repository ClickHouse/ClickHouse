import pytest

from helpers.client import QueryRuntimeException
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


def test_cluster(start_cluster):
    assert (
        node1.query(
            "SELECT hostName() FROM clusterAllReplicas('one_shard_two_nodes', system.one) ORDER BY ALL"
        )
        == "node1\nnode2\n"
    )

    assert set(
        node1.query(
            """SELECT hostName(), * FROM clusterAllReplicas("one_shard_two_nodes", system.one) ORDER BY ALL"""
        ).splitlines()
    ) == {"node1\t0", "node2\t0"}

    assert (
        node1.query("SELECT hostName() FROM cluster('one_shard_two_nodes', system.one)")
        == "node1\n"
    )
    assert (
        node2.query("SELECT hostName() FROM cluster('one_shard_two_nodes', system.one)")
        == "node2\n"
    )


def test_global_in(start_cluster):
    node1.query("DROP TABLE IF EXISTS u;")
    node1.query("CREATE TABLE u(uid Int16) ENGINE=Memory as select 0")

    assert set(
        node1.query(
            """SELECT hostName(), * FROM clusterAllReplicas("one_shard_two_nodes", system.one) where dummy GLOBAL IN u ORDER BY ALL"""
        ).splitlines()
    ) == {"node1\t0", "node2\t0"}


@pytest.mark.parametrize(
    "cluster",
    [
        pytest.param("one_shard_three_nodes"),
        pytest.param("two_shards_three_nodes"),
    ],
)
def test_skip_unavailable_replica(start_cluster, cluster):
    assert (
        node1.query(
            f"SELECT hostName() FROM clusterAllReplicas('{cluster}', system.one) ORDER BY ALL settings skip_unavailable_shards=1"
        )
        == "node1\nnode2\n"
    )


@pytest.mark.parametrize(
    "cluster",
    [
        pytest.param("one_shard_three_nodes"),
        pytest.param("two_shards_three_nodes"),
    ],
)
def test_error_on_unavailable_replica(start_cluster, cluster):
    # clusterAllReplicas() consider each replica as shard
    # so when skip_unavailable_shards=0 -  any unavailable replica should lead to an error
    with pytest.raises(QueryRuntimeException):
        node1.query(
            f"SELECT hostName() FROM clusterAllReplicas('{cluster}', system.one) ORDER BY ALL settings skip_unavailable_shards=0"
        )
