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


def test_cluster_discovery_retries_after_missing_shards_node(start_cluster):
    """
    Regression test: ClusterDiscovery must retry after a KEEPER_EXCEPTION when
    the /shards node does not exist yet at first discovery time.

    Without the fix, the retry went through the clusters loop where upsertCluster
    had no try/catch, causing the exception to propagate out of runMainThread into
    the outer backoff loop instead of retrying cleanly via the wait() mechanism.
    """
    zk = cluster.get_kazoo_client("zoo1")

    cluster_name = "test_zk_only_cluster"
    discovery_path = f"/clickhouse/discovery/{cluster_name}"

    # Clean slate, then create only the cluster root — deliberately omitting
    # /shards so the first discovery attempt on the observer hits a KEEPER_EXCEPTION.
    if zk.exists(discovery_path):
        zk.delete(discovery_path, recursive=True)
    zk.ensure_path(discovery_path)

    # Assertion 1: cluster is absent from system.clusters because /shards does
    # not exist and ClusterDiscovery cannot populate the entry.
    resp = nodes["node_observer"].query(
        f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}'"
    )
    assert resp.strip() == "0", (
        f"Cluster must not be visible when /shards is absent, got count={resp.strip()}"
    )

    # Create the complete path — /shards/<node> with a valid NodeInfo payload —
    # simulating a ClickHouse node registering itself in ZooKeeper.
    # Format mirrors ClusterDiscovery::NodeInfo::serialize(): version, address, shard_id.
    node_data = b'{"version":1,"address":"node0:9000","shard_id":1}'
    zk.create(f"{discovery_path}/shards/node0", node_data, makepath=True)

    # Assertion 2: ClusterDiscovery on the observer must recover from the earlier
    # KEEPER_EXCEPTION and discover the cluster now that /shards is populated.
    found = False
    for _ in range(30):
        resp = nodes["node_observer"].query(
            f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}'"
        )
        if resp.strip() != "0":
            found = True
            break
        time.sleep(1)

    assert found, "ClusterDiscovery did not recover after KEEPER_EXCEPTION on missing /shards node"


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
