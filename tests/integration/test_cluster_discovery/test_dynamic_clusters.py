import time
import pytest
import json

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

shard_configs = {
    "node0": ["config/config_dynamic_cluster1.xml"],
    "node1": ["config/config_dynamic_cluster1.xml"],
    "node2": ["config/config_dynamic_cluster2.xml"],
    "node3": ["config/config_dynamic_cluster3.xml"],
    "node_observer": [],
}

nodes = {
    node_name: cluster.add_instance(
        node_name,
        main_configs=shard_config + ["config/config_dynamic_cluster_observer.xml"],
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


def get_clusters_hosts(node, expected):
    count = 30
    while True:
        resp = node.query("SELECT cluster, host_name FROM system.clusters ORDER BY cluster, host_name FORMAT JSONCompact")
        hosts = json.loads(resp)["data"]
        if count <= 0 or len(hosts) == expected:
            break
        time.sleep(1)
        count -= 1
    return hosts


def test_cluster_discovery_startup_and_stop(start_cluster):
    """
    Start cluster, check nodes count in system.clusters,
    then stop/start some nodes and check that it (dis)appeared in cluster.
    """

    for node in ["node0", "node1", "node2", "node3", "node_observer"]:
        nodes[node].stop_clickhouse()

    for node in ["node0", "node1", "node2", "node_observer"]:
        nodes[node].start_clickhouse()

    expect1 = [["test_auto_cluster1", "node0"], ["test_auto_cluster1", "node1"], ["test_auto_cluster2", "node2"]]
    for node in ["node0", "node1", "node2", "node_observer"]:
        clusters = get_clusters_hosts(nodes[node], 3)
        assert clusters == expect1

    # Kill cluster test_auto_cluster2
    nodes["node2"].stop_clickhouse(kill=True)

    expect2 = [["test_auto_cluster1", "node0"], ["test_auto_cluster1", "node1"]]
    for node in ["node0", "node1", "node_observer"]:
        clusters = get_clusters_hosts(nodes[node], 2)
        assert clusters == expect2

    # Kill node in cluster test_auto_cluster1
    nodes["node1"].stop_clickhouse(kill=True)

    expect3 = [["test_auto_cluster1", "node0"]]
    for node in ["node0", "node_observer"]:
        clusters = get_clusters_hosts(nodes[node], 1)
        assert clusters == expect3

    # Restore cluster test_auto_cluster2
    nodes["node2"].start_clickhouse()

    expect4 = [["test_auto_cluster1", "node0"], ["test_auto_cluster2", "node2"]]
    for node in ["node0", "node2", "node_observer"]:
        clusters = get_clusters_hosts(nodes[node], 2)
        assert clusters == expect4

    nodes["node3"].start_clickhouse()

    expect5 = [["test_auto_cluster1", "node0"], ["test_auto_cluster2", "node2"], ["test_auto_cluster3", "node3"]]
    for node in ["node0", "node2", "node3", "node_observer"]:
        clusters = get_clusters_hosts(nodes[node], 3)
        assert clusters == expect5
