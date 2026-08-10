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


def get_clusters_hosts(node, expected, retries=30):
    while True:
        resp = node.query("SELECT cluster, host_name FROM system.clusters ORDER BY cluster, host_name FORMAT JSONCompact")
        hosts = json.loads(resp)["data"]
        if retries <= 0 or len(hosts) == expected:
            break
        time.sleep(1)
        retries -= 1
    return hosts


def wait_for_clusters_hosts(node, expected, retries=30):
    hosts = []
    while True:
        resp = node.query("SELECT cluster, host_name FROM system.clusters ORDER BY cluster, host_name FORMAT JSONCompact")
        hosts = json.loads(resp)["data"]
        if retries <= 0 or hosts == expected:
            break
        time.sleep(1)
        retries -= 1
    assert hosts == expected


def get_registration_count(node, path):
    return int(
        node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '{path}'"
        ).strip()
    )


def wait_for_registration_count(node, path, expected, attempts=30, delay=10):
    registrations = None
    last_exception = None
    for attempt in range(attempts):
        try:
            registrations = get_registration_count(node, path)
            if registrations == expected:
                return
        except Exception as ex:
            last_exception = ex

        if attempt + 1 < attempts:
            time.sleep(delay)

    raise AssertionError(
        f"Wrong ZK registration count for {path}: {registrations}, "
        f"expected: {expected}, last exception: {last_exception}"
    )


def wait_for_cluster_presence(node, cluster_name, present, retries=300):
    """
    Wait until `cluster_name` is present (or absent) in system.clusters on `node`.
    Checked per cluster instead of by total row count so the assertion does not depend
    on what other tests left running.
    """
    found = None
    for attempt in range(retries):
        found = (
            int(
                node.query(
                    f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}'"
                ).strip()
            )
            > 0
        )
        if found == present:
            return
        if attempt + 1 < retries:
            time.sleep(1)

    raise AssertionError(
        f"Cluster '{cluster_name}' present={found} on {node.name}, expected present={present}"
    )


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

    # test_auto_cluster3 was discovered dynamically after observer startup. It must be kept
    # in the observer's periodic update set, otherwise a Keeper session expiry can invalidate
    # its children watch and leave the observer with stale membership forever.
    zk_nodes = ["zoo1", "zoo2", "zoo3"]
    cluster.stop_zookeeper_nodes(zk_nodes)
    time.sleep(30)
    cluster.start_zookeeper_nodes(zk_nodes)
    cluster.wait_zookeeper_nodes_to_start(zk_nodes)

    for path in [
        "/clickhouse/discovery/test_auto_cluster1/shards",
        "/clickhouse/discovery/test_auto_cluster2/shards",
        "/clickhouse/discovery2/test_auto_cluster3/shards",
    ]:
        wait_for_registration_count(nodes["node_observer"], path, 1)

    nodes["node3"].stop_clickhouse(kill=True)

    expect6 = [["test_auto_cluster1", "node0"], ["test_auto_cluster2", "node2"]]
    wait_for_clusters_hosts(nodes["node_observer"], expect6, retries=300)


def test_dynamic_cluster_recovers_after_empty_then_session_expiry(start_cluster):
    """
    Regression test: a one-node dynamic cluster that goes empty BEFORE a Keeper session
    expiry must still be rediscovered when its node comes back.

    Bug path (without the fix):
    1. The only node of a dynamic cluster leaves. `upsertCluster` sees an empty node list and
       calls `removeCluster`, which drops the `clusters_to_update` entry and the watch callback
       but leaves the `clusters_info` record behind.
    2. The Keeper session expires, so the children watch that would otherwise have resurrected
       the cluster is gone too.
    3. The root rescan still sees the (persistent) cluster znode, but dedups it against the
       stale `clusters_info` record, so it ends up in neither `clusters_to_insert` nor the
       update set and is never polled again.
    4. The node comes back and re-registers, but nothing on the observer notices. The cluster
       stays missing from system.clusters indefinitely.

    With the fix `removeCluster` erases the `clusters_info` record as well, so the rescan can
    rediscover the cluster once nodes register under it again.
    """
    observer = nodes["node_observer"]
    zk_nodes = ["zoo1", "zoo2", "zoo3"]
    cluster3_shards = "/clickhouse/discovery2/test_auto_cluster3/shards"

    # Bring the one-node dynamic cluster up and confirm the observer discovered it.
    nodes["node3"].start_clickhouse()
    wait_for_cluster_presence(observer, "test_auto_cluster3", True)

    # Take its only node away, so the cluster goes empty *before* any session expiry.
    # This is what makes the observer call removeCluster for a dynamic cluster.
    nodes["node3"].stop_clickhouse(kill=True)
    wait_for_cluster_presence(observer, "test_auto_cluster3", False)

    # Expire every Keeper session. The cluster znode itself is persistent and survives;
    # only the ephemeral registration underneath it is gone.
    cluster.stop_zookeeper_nodes(zk_nodes)
    time.sleep(30)
    cluster.start_zookeeper_nodes(zk_nodes)
    cluster.wait_zookeeper_nodes_to_start(zk_nodes)

    # The node returns and registers again.
    nodes["node3"].start_clickhouse()
    wait_for_registration_count(observer, cluster3_shards, 1)

    # The observer must rediscover the cluster. Allow well over force_update_interval (2 min),
    # since after a session expiry recovery depends on the periodic root rescan rather than
    # on a watch firing.
    wait_for_cluster_presence(observer, "test_auto_cluster3", True, retries=300)
