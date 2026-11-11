#!/usr/bin/env python3
"""
Regression test for https://github.com/ClickHouse/ClickHouse/issues/91464.

node3 is wiped and restarted in `force_recovery`, so it self-elects leader
on nothing but its own (near-empty) log. node1/node2 stay healthy and hold
all previously committed data. node3 then sends `append_entries` that
conflicts with their committed log.

Before the fix, node1/node2 aborted with "Trying to rollback invalid ZXID".
After the fix, they deny the request and increment the
`KeeperRejectedCommittedLogRollback` profile event instead.

The term at which node3 self-elects races the first heartbeat from the
healthy leader. At the same term, no conflict occurs and recovery
completes, so the test restarts node3 (without wiping) to reach a higher
term that does conflict. Recovery completes as soon as the peers answer
heartbeats, even when they deny node3's log, so node3 being leader does not
mean that no conflict occurred.

node3 stays running afterwards and node1/node2 deny all its requests of that
term, including heartbeats. Denying is not enough for the pair to keep serving
though: `force_recovery` lets node3 win every election on its own, so every
term it manages to deliver steps the pair's leader down again. A leader that
lives for less than the session timeout cannot serve a client, so the pair can
never keep a leader while it hears node3. The test therefore checks the
denials while node3 is still connected, then cuts node3 off from the pair,
keeping its process running, and only then checks that the pair elects a
leader and serves the old data and a new write.
"""

import os
import time

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ROLLBACK_ERROR_SIGNATURE = "Trying to rollback invalid ZXID"
REJECTION_PROFILE_EVENT = "KeeperRejectedCommittedLogRollback"
REJECTION_LOG_SIGNATURE = "which would have rolled back already committed logs"

NEW_ZNODE = "/test_committed_rollback_after_rejection"

TEST_ZNODES = [(f"/test_committed_rollback_{i}", f"data{i}".encode()) for i in range(10)]


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def assert_healthy_nodes_running():
    for node in (node1, node2):
        assert node.get_process_pid("clickhouse") is not None, (
            f"{node.name} is not running anymore; it likely crashed while trying "
            f"to roll back already-committed log entries (the bug this test "
            f"guards against)"
        )


def retry_until_success(action, nodename, timeout=120.0):
    """Run `action(zk)` on a fresh client of `nodename` until it succeeds."""
    deadline = time.time() + timeout
    while True:
        try:
            zk = get_fake_zk(nodename)
            try:
                return action(zk)
            finally:
                zk.stop()
                zk.close()
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(0.5)


def wait_for_leader(nodes, timeout=60.0, settle=2.0):
    """Wait until one node of `nodes` is leader and still is `settle` seconds later.

    One sample is not enough: a leader seen right before node3 delivers another
    term is gone a moment later, and `settle` is longer than node3's election
    timeout, so it rejects such a transient leader.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            leader = keeper_utils.get_leader(cluster, nodes)
        except Exception:
            time.sleep(0.2)
            continue
        time.sleep(settle)
        if keeper_utils.is_leader(cluster, leader):
            return leader
    raise Exception(f"node1/node2 did not elect a stable leader in {timeout} seconds")


def isolate_node3():
    """Cut node3 off from node1/node2, keeping its process running.

    A one-way drop is not enough: TCP needs both directions, so node1/node2
    would stop receiving node3's requests as well. Cutting node3 off is also
    what makes the pair's liveness deterministic, because every term node3
    manages to deliver steps the pair's leader down again. Returns the rules to
    heal on exit.
    """
    partition = PartitionManager()
    for node in (node1, node2):
        partition.partition_instances(node3, node)
    return partition


def get_rejecting_node():
    for node in (node1, node2):
        try:
            events = keeper_utils.get_profile_events(cluster, node)
        except Exception:
            continue
        if events.get(REJECTION_PROFILE_EVENT, 0) > 0:
            return node
    return None


def restart_node3_in_force_recovery(wipe_coordination_dir, window=30.0):
    """Restart node3 in `force_recovery`. Return True if node1/node2 rejected a
    conflicting request within `window` seconds (without the fix they abort).
    """
    node3.stop_clickhouse()
    if wipe_coordination_dir:
        node3.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination"])
    node3.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper3_recovery.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper3.xml",
    )
    node3.start_clickhouse()

    start = time.time()
    while time.time() - start < window:
        assert_healthy_nodes_running()
        if get_rejecting_node() is not None:
            return True
        time.sleep(0.5)
    return False


def test_refuse_committed_log_rollback():
    try:
        cluster.start()

        keeper_utils.wait_nodes(cluster, [node1, node2, node3])

        # Write data and check it is visible on all three nodes.
        zk1 = get_fake_zk("node1")
        for path, data in TEST_ZNODES:
            zk1.create(path, data)

        for nodename in ("node1", "node2", "node3"):
            zk = get_fake_zk(nodename)
            for path, data in TEST_ZNODES:
                assert zk.get(path)[0] == data
            zk.stop()
            zk.close()

        zk1.stop()
        zk1.close()

        # Wipe and restart node3 in `force_recovery`; retry without wiping
        # until it self-elects at a conflicting term. See the module docstring.
        for cycle in range(3):
            if restart_node3_in_force_recovery(wipe_coordination_dir=(cycle == 0)):
                break
        else:
            raise Exception("node3 did not cause a conflicting log in 3 restarts")

        surviving_node = get_rejecting_node()
        assert surviving_node is not None

        # The pre-fix signature must never appear.
        for node in (node1, node2):
            assert not node.contains_in_log(ROLLBACK_ERROR_SIGNATURE), (
                f"{node.name} logged '{ROLLBACK_ERROR_SIGNATURE}', meaning it "
                f"attempted to roll back committed data - the fix did not take effect"
            )

        assert surviving_node.contains_in_log(REJECTION_LOG_SIGNATURE), (
            f"{surviving_node.name} did not log the expected rejection message"
        )

        assert_healthy_nodes_running()

        # node3 keeps running. Cut it off from node1/node2 first: while it can
        # still reach them, every term it wins on its own steps the pair's
        # leader down again and no client session survives. See the module
        # docstring.
        partition = isolate_node3()
        try:
            wait_for_leader((node1, node2))

            # Previously written data stays readable on both remaining nodes.
            def read_test_znodes(zk):
                for path, data in TEST_ZNODES:
                    assert zk.get(path)[0] == data

            for nodename in ("node1", "node2"):
                retry_until_success(read_test_znodes, nodename)

            # {node1, node2} must still form a quorum and accept new writes.
            # A retry can follow a create that already went through.
            retry_until_success(
                lambda zk: zk.exists(NEW_ZNODE) or zk.create(NEW_ZNODE, b"still-alive"),
                "node1",
            )
            assert (
                retry_until_success(lambda zk: zk.get(NEW_ZNODE)[0], "node2")
                == b"still-alive"
            )

            assert_healthy_nodes_running()
            assert node3.get_process_pid("clickhouse") is not None, (
                "node3 must keep running: only its network link to node1/node2 is "
                "cut off, the process itself is not stopped"
            )
        finally:
            partition.heal_all()

    finally:
        cluster.shutdown()
