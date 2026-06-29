"""
Integration test for the Keeper changelog per-peer read-ahead feature.

Scenario:
  1. Configure a 3-node Keeper cluster with a small log cache (forces entries to
     disk quickly) and log file rotation every 1000 entries.
  2. Start only nodes 1 and 2 (quorum = 2 of 3).
  3. Write 5000 znodes via kazoo — produces at least 5 sealed log files on the leader.
  4. Start node 3, which has no log and must catch up by streaming log entries from
     the leader via log_entries_ext.
  5. Wait for node 3 to become a connected follower.
  6. Assert that:
     a. Node 3 can read back all the znodes written in step 3 (correctness).
     b. The read-ahead fill counter on node 3 is > 0, confirming that the
        per-peer decoded read-ahead path was exercised during catch-up.
"""

import time

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


def read_ahead_decoded_entries(node):
    """Return the cumulative KeeperLogsReadAheadFillDecodedEntries counter from system.events."""
    result = node.query(
        "SELECT value FROM system.events WHERE event = 'KeeperLogsReadAheadFillDecodedEntries'",
        query_id="readahead_check",
    )
    return int(result.strip()) if result.strip() else 0


def test_readahead_catchup(started_cluster):
    """
    Write 5000 entries on a 2-node quorum, then start the lagging 3rd node and
    verify it catches up correctly using the read-ahead path.
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

    # Confirm the leader has sealed at least 4 log files (5000 / 1000 = 5 files).
    leader = node1 if keeper_utils.is_leader(cluster, node1) else node2
    log_files = (
        leader.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
        .strip()
        .split("\n")
    )
    assert len(log_files) >= 4, (
        f"Expected at least 4 log files, got {len(log_files)}: {log_files}"
    )

    # --- Step 2: capture baseline counters, then start node3 ---
    # node2 is already an active follower during step 1 and can accumulate
    # KeeperLogsReadAheadFillDecodedEntries before node3 starts; assert a delta.
    def get_counter(node, event):
        r = node.query(f"SELECT value FROM system.events WHERE event = '{event}'")
        return int(r.strip()) if r.strip() else 0

    baseline_decoded = get_counter(leader, "KeeperLogsReadAheadFillDecodedEntries")
    baseline_cursors = get_counter(leader, "KeeperLogsReadAheadCursorsInstalled")
    print(
        f"Leader read-ahead baseline (before node3 start): "
        f"decoded={baseline_decoded}, cursors_installed={baseline_cursors}"
    )

    node3.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node3)

    # Poll until the decoded-entries counter grows beyond the baseline.
    deadline = time.time() + 30
    decoded_delta = 0
    while time.time() < deadline:
        decoded_delta = get_counter(leader, "KeeperLogsReadAheadFillDecodedEntries") - baseline_decoded
        if decoded_delta > 0:
            break
        time.sleep(0.5)

    cursors_delta = get_counter(leader, "KeeperLogsReadAheadCursorsInstalled") - baseline_cursors
    reopens = get_counter(leader, "KeeperLogsReadAheadFillReopens")
    print(
        f"Leader read-ahead delta after node3 catch-up: "
        f"decoded_delta={decoded_delta}, cursors_delta={cursors_delta}, reopens={reopens}"
    )
    assert decoded_delta > 0, (
        "KeeperLogsReadAheadFillDecodedEntries did not increase after node3 started: "
        "read-ahead did not fire when streaming to the lagging follower"
    )

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
