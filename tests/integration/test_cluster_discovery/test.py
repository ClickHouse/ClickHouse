import functools

import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

shard_configs = {
    "node0": "config/config.xml",
    "node1": "config/config_shard1.xml",
    "node2": "config/config.xml",
    "node3": "config/config_shard3.xml",
    "node4": "config/config.xml",
    "node_observer": "config/config_observer.xml",
}

nodes = {
    node_name: cluster.add_instance(
        node_name,
        main_configs=[shard_config],
        stay_alive=True,
        with_zookeeper=True,
    )
    for node_name, shard_config in shard_configs.items()
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cluster_discovery_startup_and_stop(start_cluster):
    """
    Start cluster, check nodes count in system.clusters,
    then stop/start some nodes and check that it (dis)appeared in cluster.
    """

    check_nodes_count = functools.partial(
        check_on_cluster, what="count()", msg="Wrong nodes count in cluster"
    )
    check_shard_num = functools.partial(
        check_on_cluster,
        what="count(DISTINCT shard_num)",
        msg="Wrong shard_num count in cluster",
    )

    # `- 1` because one node is an observer
    total_shards = len(set(shard_configs.values())) - 1
    total_nodes = len(nodes) - 1

    check_nodes_count(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_nodes
    )
    check_shard_num(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_shards
    )

    # test ON CLUSTER query
    nodes["node0"].query(
        "CREATE TABLE tbl ON CLUSTER 'test_auto_cluster' (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    nodes["node0"].query("INSERT INTO tbl VALUES (1)")
    nodes["node1"].query("INSERT INTO tbl VALUES (2)")

    assert (
        int(
            nodes["node_observer"]
            .query(
                "SELECT sum(x) FROM clusterAllReplicas(test_auto_cluster, default.tbl)"
            )
            .strip()
        )
        == 3
    )

    # Query SYSTEM DROP DNS CACHE may reload cluster configuration
    # check that it does not affect cluster discovery
    nodes["node1"].query("SYSTEM DROP DNS CACHE")
    nodes["node0"].query("SYSTEM DROP DNS CACHE")

    check_shard_num(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_shards
    )

    nodes["node1"].stop_clickhouse(kill=True)
    check_nodes_count(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_nodes - 1
    )

    # node1 was the only node in shard '1'
    check_shard_num(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_shards - 1
    )

    nodes["node3"].stop_clickhouse()
    check_nodes_count(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_nodes - 2
    )

    nodes["node1"].start_clickhouse()
    check_nodes_count(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_nodes - 1
    )

    nodes["node3"].start_clickhouse()
    check_nodes_count(
        [nodes["node0"], nodes["node2"], nodes["node_observer"]], total_nodes
    )

    # regular cluster is not affected
    check_nodes_count(
        [nodes["node1"], nodes["node2"]], 2, cluster_name="two_shards", retries=1
    )
