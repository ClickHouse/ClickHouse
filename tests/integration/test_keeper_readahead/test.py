"""
Integration test for the Keeper changelog per-peer read-ahead feature.

Scenario:
  1. Configure a 3-node Keeper cluster with a small log cache (forces entries to
     disk quickly) and log file rotation every 1000 entries.
  2. Start only nodes 1 and 2 (quorum = 2 of 3).
  3. Write 5000 znodes via kazoo — produces ~5 log files on the leader (at least 4 sealed).
  4. Start node 3, which has no log and must catch up by streaming log entries from
     the leader via log_entries_ext.
  5. Wait for node 3 to become a connected follower.
  6. Assert that node 3 can read back all the znodes written in step 3.
  7. Assert via `pfev` deltas (summed over node1 + node2) that the read-ahead
     machinery actually fired during catch-up, so a silent fallback to direct
     reads can't pass this test. Commit read-ahead is disabled in the cluster
     config, so the increase can only come from node3's per-peer catch-up.
"""

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# node1 is configured as the preferred leader (priority 3).
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_zookeeper=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_zookeeper=False,
)
# node3 starts stopped; we bring it up after the writes to simulate a lagging follower.
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_zookeeper=False,
)

NUM_ZNODES = 5000  # must exceed rotate_log_storage_interval * 2 to produce multiple sealed files
ZNODE_ROOT = "/readahead_test"
ZNODE_VALUE = b"v" * 64  # small fixed-size payload for predictable log sizes


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def test_readahead_catchup(started_cluster):
    """
    Write 5000 entries on a 2-node quorum, then start the lagging 3rd node and
    verify it catches up correctly with read-ahead enabled.
    """
    # --- Setup: bring node3 down so it misses all writes ---
    node3.stop_clickhouse()

    # Clean node3's coordination data so it starts with an empty log.
    node3.exec_in_container(
        ["rm", "-rf", "/var/lib/clickhouse/coordination/log"]
    )
    node3.exec_in_container(
        ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
    )

    # Wait for the 2-node quorum (node1 + node2) to elect a leader.
    keeper_utils.wait_nodes(cluster, [node1, node2])

    # --- Step 1: write NUM_ZNODES znodes on the quorum ---
    zk = get_zk(node1)
    try:
        zk.create(ZNODE_ROOT)
        for i in range(NUM_ZNODES):
            zk.create(f"{ZNODE_ROOT}/node_{i:06d}", ZNODE_VALUE)
    finally:
        zk.stop()
        zk.close()

    # 5000 znodes / 1000 per file ~= 5 files, but the newest may still be active; assert >= 4 sealed.
    leader = node1 if keeper_utils.is_leader(cluster, node1) else node2
    log_files = (
        leader.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
        .strip()
        .split("\n")
    )
    assert len(log_files) >= 4, (
        f"Expected at least 4 log files, got {len(log_files)}: {log_files}"
    )

    # Sum over both quorum members so a re-election during catch-up doesn't lose the delta.
    def sum_profile_event(name):
        total = 0
        for node in (node1, node2):
            total += keeper_utils.get_profile_events(cluster, node).get(name, 0)
        return total

    fill_decoded_before = sum_profile_event("KeeperLogsReadAheadFillDecodedEntries")
    cursors_installed_before = sum_profile_event("KeeperLogsReadAheadCursorsInstalled")

    # --- Step 2: start node3 (empty log) and wait for it to catch up ---
    node3.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node3)

    # --- Step 3: verify correctness — node3 must serve every written znode ---
    zk3 = get_zk(node3)
    try:
        children = zk3.get_children(ZNODE_ROOT)
        assert len(children) == NUM_ZNODES, (
            f"node3 has {len(children)} children, expected {NUM_ZNODES}"
        )
        # Spot-check a sample of values.
        for i in range(0, NUM_ZNODES, NUM_ZNODES // 20):
            data, _ = zk3.get(f"{ZNODE_ROOT}/node_{i:06d}")
            assert data == ZNODE_VALUE, (
                f"Wrong value at node_{i:06d}: {data!r}"
            )
    finally:
        zk3.stop()
        zk3.close()

    # --- Step 4: confirm read-ahead actually engaged for node3's catch-up ---
    fill_decoded_after = sum_profile_event("KeeperLogsReadAheadFillDecodedEntries")
    cursors_installed_after = sum_profile_event("KeeperLogsReadAheadCursorsInstalled")

    assert fill_decoded_after - fill_decoded_before > 0, (
        "Expected KeeperLogsReadAheadFillDecodedEntries to increase across node1+node2 "
        f"during catch-up, before={fill_decoded_before}, after={fill_decoded_after}"
    )
    assert cursors_installed_after - cursors_installed_before > 0, (
        "Expected KeeperLogsReadAheadCursorsInstalled to increase across node1+node2 "
        f"during catch-up, before={cursors_installed_before}, after={cursors_installed_after}"
    )
