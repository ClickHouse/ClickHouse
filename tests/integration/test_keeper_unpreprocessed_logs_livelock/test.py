#!/usr/bin/env python3
"""
Regression test for a NuRaft livelock that happens when a follower restarts with
un-preprocessed local logs and is behind the leader.

Background
----------
With `async_replication` + `nuraft_streaming_mode` + `use_new_dispatcher` (the
defaults), the follower handles `append_entries` on the pipelined,
parallel-log-appending path. A freshly restarted node whose on-disk logs are not
covered by a snapshot has `localLogsPreprocessed() == false` until it manages to
commit those logs. While in that state, `KeeperServer::callbackFunc`
(`GotAppendEntryReqFromLeader`) clears the log entries of every incoming
`append_entries` request. The (now empty) request then went down a code path in
`handle_append_entries` that deferred the response until the entries became
durable -- but for an empty request that durability point
(`last_durable_index`, which starts at 0 after restart) is never reached, so the
response was never sent.

The leader therefore never gets an `append_entries` response from the follower:
its RPC client to the follower stays "busy" with in-flight bytes that are never
cleared, it can never reach quorum, and after the leadership-expiry timeout it
yields leadership and immediately re-wins (its log is longer). The cluster
re-elects forever and never starts serving requests -- a livelock that does not
recover on its own.

The fix makes `handle_append_entries` wait for durability of an empty request
only when there are already pending (deferred) follower responses, so an empty
request no longer starts an unsatisfiable durability wait.

Reproduction recipe (mirrors the original keeper-bench setup)
-------------------------------------------------------------
3 servers are configured but only 2 are ever running at the same time at the
end, so that quorum requires the stuck follower. We give the eventual leader a
strictly longer log than the eventual follower, so that the leader has to send a
non-empty catch-up `append_entries` (which then gets cleared on the follower).

Without the fix this test hangs in `wait_until_connected` (the cluster never
starts serving); with the fix the cluster recovers quickly.
"""

import logging
import time
from multiprocessing.dummy import Pool

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)

ALL_NODES = [node1, node2, node3]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def wait_for_leader(nodes, timeout=30.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            return keeper_utils.get_leader(cluster, nodes)
        except Exception:
            time.sleep(0.5)
    raise Exception("No leader elected within timeout")


def start_and_connect(node):
    # NOTE: start_clickhouse() waits for the server to become ready, and for a
    # Keeper node that means waiting for the Raft quorum to form. The quorum
    # here needs both restarted nodes, so they must be started in parallel
    # (Pool.map below) -- otherwise the first start_clickhouse() blocks forever
    # waiting for a quorum that can't exist until the second node is up.
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


def test_unpreprocessed_logs_livelock(started_cluster):
    keeper_utils.wait_nodes(cluster, ALL_NODES)

    # 1) Baseline entry committed on all three nodes.
    leader = wait_for_leader(ALL_NODES)
    zk = get_fake_zk(leader)
    try:
        zk.create("/test")
    finally:
        zk.stop()
        zk.close()

    # Make sure node2 actually persisted the entry: after its restart there must
    # be at least one on-disk log entry that is not covered by a snapshot, so
    # that localLogsPreprocessed() == false and the buggy code path is taken.
    node2_zk = get_fake_zk(node2)
    try:
        for _ in range(60):
            if node2_zk.exists("/test"):
                break
            time.sleep(0.5)
        else:
            raise Exception("node2 did not replicate /test")
    finally:
        node2_zk.stop()
        node2_zk.close()

    # 2) Take node2 down and append more logs using node1 + node3 (quorum = 2).
    #    node2 is now strictly behind node1/node3.
    node2.stop_clickhouse()

    leader = wait_for_leader([node1, node3])
    zk = get_fake_zk(leader)
    try:
        zk.create("/test/n1n3")
    finally:
        zk.stop()
        zk.close()

    # 3) Stop the whole cluster. create_snapshot_on_exit=false means the on-disk
    #    logs are not covered by a snapshot, so on the next startup every node
    #    has un-preprocessed logs (localLogsPreprocessed() == false).
    node1.stop_clickhouse()
    node3.stop_clickhouse()

    # Sanity: there are real changelog files (logs that need preprocessing) and
    # no snapshot for node1 (the eventual leader).
    log_files = node1.exec_in_container(
        ["bash", "-c", "ls /var/lib/clickhouse/coordination/log 2>/dev/null || true"]
    ).strip()
    logging.info("node1 log files: %s", log_files)
    assert log_files != ""

    # 4) Bring up only node1 (longer log -> becomes leader) and node2 (behind ->
    #    becomes follower). node3 stays down, so quorum requires node2, which is
    #    exactly the node that gets stuck.
    #
    #    With the bug, node1 keeps re-electing itself and Keeper never starts
    #    serving (the catch-up append to node2 is cleared, then deferred on a
    #    durability point that is never reached, so it is never answered). Both
    #    start_clickhouse() calls then time out waiting for readiness and this
    #    raises. With the fix the cluster forms a quorum and starts serving
    #    within a few seconds.
    #
    #    The two nodes must be started in parallel: start_clickhouse() waits for
    #    Keeper readiness (quorum), which can only form once both are up.
    p = Pool(2)
    try:
        p.map(start_and_connect, [node1, node2])
    finally:
        p.close()
        p.join()

    # 5) The recovered cluster is fully functional: previously committed data is
    #    present and new writes succeed.
    leader = wait_for_leader([node1, node2])
    zk = get_fake_zk(leader)
    try:
        children = zk.get_children("/test")
        assert "n1n3" in children
        zk.create("/test/after_recovery", b"ok")
        assert zk.exists("/test/after_recovery") is not None
    finally:
        zk.stop()
        zk.close()
