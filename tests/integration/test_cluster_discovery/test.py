import pytest

import functools
import time

from helpers.cluster import ClickHouseCluster

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


def check_on_cluster(
    nodes, expected, *, what, cluster_name="test_auto_cluster", msg=None, retries=5
):
    """
    Select data from `system.clusters` on specified nodes and check the result
    """
    assert 1 <= retries <= 6

    node_results = {}
    for retry in range(1, retries + 1):
        for node in nodes:
            if node_results.get(node.name) == expected:
                # do not retry node after success
                continue
            query_text = (
                f"SELECT {what} FROM system.clusters WHERE cluster = '{cluster_name}'"
            )
            node_results[node.name] = int(node.query(query_text))

        if all(actual == expected for actual in node_results.values()):
            break

        print(f"Retry {retry}/{retries} unsuccessful, result: {node_results}")

        if retry != retries:
            time.sleep(2**retry)
    else:
        msg = msg or f"Wrong '{what}' result"
        raise Exception(
            f"{msg}: {node_results}, expected: {expected} (after {retries} retries)"
        )


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
