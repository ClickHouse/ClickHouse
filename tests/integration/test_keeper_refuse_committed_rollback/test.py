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
"""

import os
import time

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

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

        # The operator fixes the mistake; node3 could otherwise keep
        # disrupting leader election.
        node3.stop_clickhouse()

        # Previously written data must still be readable from both nodes.
        for nodename in ("node1", "node2"):
            zk = get_fake_zk(nodename)
            for path, data in TEST_ZNODES:
                assert zk.get(path)[0] == data
            zk.stop()
            zk.close()

        # {node1, node2} must still form a quorum and accept new writes.
        zk1 = get_fake_zk("node1")
        zk1.create("/test_committed_rollback_after_rejection", b"still-alive")
        zk1.stop()
        zk1.close()

        zk2 = get_fake_zk("node2")
        assert zk2.get("/test_committed_rollback_after_rejection")[0] == b"still-alive"
        zk2.stop()
        zk2.close()

    finally:
        cluster.shutdown()
