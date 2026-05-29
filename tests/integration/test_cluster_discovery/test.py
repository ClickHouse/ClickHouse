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
        main_configs=[shard_config, "config/config_discovery_path.xml", "config/macros.xml"],
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
                "SELECT sum(x) FROM clusterAllReplicas(test_auto_cluster, default.tbl) settings serialize_query_plan=0"
            )
            .strip()
        )
        == 3
    )

    # Query SYSTEM CLEAR DNS CACHE may reload cluster configuration
    # check that it does not affect cluster discovery
    nodes["node1"].query("SYSTEM CLEAR DNS CACHE")
    nodes["node0"].query("SYSTEM CLEAR DNS CACHE")

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

    # cleanup
    nodes["node0"].query("DROP TABLE tbl ON CLUSTER 'test_auto_cluster' SYNC")


def test_cluster_discovery_no_crash_on_empty_static_cluster(start_cluster):
    """
    Regression test: server must not crash when a static cluster temporarily has no nodes.

    Crash path (without fix):
    1. All member nodes stop → ZK ephemeral nodes deleted → observer detects empty cluster.
    2. upsertCluster sees nodes_info.empty() → calls removeCluster() → erases watch callback
       from get_nodes_callbacks.
    3. A member node restarts → old ZK watch fires → cluster re-queued for update.
    4. upsertCluster is called again → on_exit calls getNodeNames(set_callback=True).
    5. Callback not found, zk_root_index==0 → multicluster_discovery_paths[0-1] OOB → crash.
    """
    import time

    observer = nodes["node_observer"]
    member_nodes = [nodes[n] for n in ("node0", "node1", "node2", "node3", "node4")]

    # Wait for cluster to be fully up before the test
    check_on_cluster(
        [observer], len(member_nodes), what="count()", msg="Pre-test cluster count wrong"
    )

    # Stop all member nodes gracefully — closes ZK sessions immediately,
    # deleting their ephemeral registration nodes in ZK.
    for node in member_nodes:
        node.stop_clickhouse()

    # Wait for the observer to detect the now-empty cluster.
    # This triggers upsertCluster → nodes_info.empty() → removeCluster() erasing the
    # watch callback (the step that sets up the crash on the subsequent call).
    check_on_cluster(
        [observer], 0, what="count()", msg="Cluster should appear empty to observer"
    )

    assert observer.query("SELECT 1").strip() == "1", "Observer crashed after cluster became empty"

    # Restart one member node. It registers in ZK, which triggers the old watch callback
    # that was stored in the ZK client (independently of get_nodes_callbacks).
    # On unfixed code this triggers the second upsertCluster call that crashes.
    nodes["node0"].start_clickhouse()
    time.sleep(10)

    assert observer.query("SELECT 1").strip() == "1", "Observer crashed after member node restarted"

    # Restore all nodes for subsequent tests
    for node in member_nodes[1:]:
        node.start_clickhouse()

    check_on_cluster(
        [observer], len(member_nodes), what="count()", msg="Cluster should recover after restart"
    )


def test_cluster_discovery_keeper_restart(start_cluster):
    """
    Regression test: data nodes must re-register ephemeral discovery znodes
    after Keeper session expiry caused by Keeper restart.

    Regression introduced in b932cce8805 (v25.12) which removed the timeout
    from Flags::wait(), causing the main loop to block forever after session
    expiry when ZK watches are invalidated.

    Bug path (without fix):
    1. All Keeper nodes restart -> all client sessions expire -> ephemeral znodes deleted.
    2. ClickHouse data nodes reconnect to Keeper with new sessions.
    3. ClusterDiscovery::runMainThread is blocked on Flags::wait() -- ZK watches were
       invalidated on the old session, so the condition variable is never notified.
    4. force_update_interval check and re-registration logic exist but are unreachable.
    5. Observer nodes see an incomplete/empty cluster indefinitely.

    With fix: Flags::wait() uses wait_for() with a timeout, so the main loop unblocks
    periodically and re-registers the node.
    """
    import time

    observer = nodes["node_observer"]
    member_nodes = [nodes[n] for n in ("node0", "node1", "node2", "node3", "node4")]
    total_nodes = len(member_nodes)

    def get_registration_count():
        return int(
            observer.query(
                "SELECT count() FROM system.zookeeper "
                "WHERE path = '/clickhouse/discovery/test_auto_cluster/shards'"
            ).strip()
        )

    def wait_for_registration_count(expected, attempts=30, delay=10):
        registrations = None
        last_exception = None
        for attempt in range(attempts):
            try:
                registrations = get_registration_count()
                if registrations == expected:
                    return
            except Exception as ex:
                last_exception = ex

            if attempt + 1 < attempts:
                time.sleep(delay)

        raise AssertionError(
            f"Wrong ZK registration count: {registrations}, "
            f"expected: {expected}, last exception: {last_exception}"
        )

    # Verify cluster is fully up, both in observer cache and in Keeper registrations.
    check_on_cluster(
        [observer], total_nodes, what="count()", msg="Pre-test cluster count wrong"
    )
    wait_for_registration_count(total_nodes, attempts=6, delay=2)

    # Stop all ZK nodes to force session expiry.
    zk_nodes = ["zoo1", "zoo2", "zoo3"]
    cluster.stop_zookeeper_nodes(zk_nodes)

    # Wait long enough for session timeout to expire (default 10-30s).
    time.sleep(30)

    # Restart ZK -- sessions are expired, ephemeral znodes are gone.
    cluster.start_zookeeper_nodes(zk_nodes)
    cluster.wait_zookeeper_nodes_to_start(zk_nodes)

    # With the fix, re-registration should happen within force_update_interval (2 min).
    # Check Keeper directly instead of system.clusters because existing observers can keep
    # stale cluster_impls cached even when ephemeral znodes are missing.
    wait_for_registration_count(total_nodes)

    # Restart the observer to drop the old in-memory system.clusters cache, then verify
    # a fresh observer sees the re-registered nodes from Keeper.
    observer.restart_clickhouse()
    check_on_cluster(
        [observer],
        total_nodes,
        what="count()",
        msg="Fresh observer should see all nodes after Keeper restart",
        retries=6,
    )


def test_cluster_discovery_macros(start_cluster):
    # wait for all nodes to be started
    check_nodes_count = functools.partial(
        check_on_cluster, what="count()", msg="Wrong nodes count in cluster"
    )
    total_nodes = len(nodes) - 1
    check_nodes_count([nodes["node_observer"]], total_nodes)

    # check macros
    res = nodes["node_observer"].query(
        "SELECT sum(number) FROM clusterAllReplicas('{autocluster}', system.numbers) WHERE number=1"
    )
    assert res.strip() == "5"
