import pytest

import functools
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

shard_configs = {
    0: "config/config.xml",
    1: "config/config_shard1.xml",
    2: "config/config.xml",
    3: "config/config_shard3.xml",
    4: "config/config.xml",
    'observer': "config/config_observer.xml",
}

nodes = {
    node_id: cluster.add_instance(
        f"node_{node_id}",
        main_configs=[shard_config],
        stay_alive=True,
        with_zookeeper=True,
    )
    for node_id, shard_config in shard_configs.items()
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

    for retry in range(1, retries + 1):
        nodes_res = {
            node.name: int(
                node.query(
                    f"SELECT {what} FROM system.clusters WHERE cluster = '{cluster_name}'"
                )
            )
            for node in nodes
        }
        if all(actual == expected for actual in nodes_res.values()):
            break

        if retry != retries:
            time.sleep(2**retry)
    else:
        msg = msg or f"Wrong '{what}' result"
        raise Exception(
            f"{msg}: {nodes_res}, expected: {expected} (after {retries} retries)"
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

    total_shards = 3
    # one node is observer
    total_nodes = len(nodes) - 1
    check_nodes_count([nodes[0], nodes[2], nodes['observer']], total_nodes)
    check_shard_num([nodes[0], nodes[2], nodes['observer']], total_shards)

    nodes[1].stop_clickhouse(kill=True)
    check_nodes_count([nodes[0], nodes[2], nodes['observer']], total_nodes - 1)

    # node_1 was the only node in shard '1'
    check_shard_num([nodes[0], nodes[2], nodes['observer']], total_shards - 1)

    nodes[3].stop_clickhouse()
    check_nodes_count([nodes[0], nodes[2], nodes['observer']], total_nodes - 2)

    nodes[1].start_clickhouse()
    check_nodes_count([nodes[0], nodes[2], nodes['observer']], total_nodes - 1)

    nodes[3].start_clickhouse()
    check_nodes_count([nodes[0], nodes[2], nodes['observer']], total_nodes)

    # regular cluster is not affected
    check_nodes_count([nodes[1], nodes[2]], 2, cluster_name="two_shards", retries=1)
