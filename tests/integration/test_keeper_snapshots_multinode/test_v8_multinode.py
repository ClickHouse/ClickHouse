#!/usr/bin/env python3
"""Integration test for V8 snapshot format in a 3-node Keeper cluster.

Scenario:
  1. Start a 3-node cluster with `write_snapshot_version=8` and `snapshot_distance=10`.
  2. Stop one follower so it will miss the initial writes.
  3. Write 12 nodes via the leader — this triggers a V8 snapshot.
  4. Restart the stopped follower.
  5. The leader sends the V8 snapshot to the follower (NuRaft snapshot transfer).
  6. Poll `lgif` 4LW on the follower until `last_snapshot_idx` advances.
  7. Verify that all written nodes are readable on the rejoined follower.
"""

import csv
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Use distinct node names (v8node*) to avoid docker network conflicts when
# both test files in this directory run in parallel.
v8node1 = cluster.add_instance(
    "v8node1",
    main_configs=["configs/enable_keeper1_v8.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
v8node2 = cluster.add_instance(
    "v8node2",
    main_configs=["configs/enable_keeper2_v8.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
v8node3 = cluster.add_instance(
    "v8node3",
    main_configs=["configs/enable_keeper3_v8.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ALL_V8_NODES = [v8node1, v8node2, v8node3]


def wait_v8_nodes():
    keeper_utils.wait_nodes(cluster, ALL_V8_NODES)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except Exception:
        pass


def _parse_lgif(raw):
    """Parse tab-separated lgif output into a dict."""
    result = {}
    reader = csv.reader(raw.split("\n"), delimiter="\t")
    for row in reader:
        if len(row) == 2:
            result[row[0]] = row[1]
    return result


def _get_last_snapshot_idx(node, timeout=30):
    """Poll lgif until last_snapshot_idx is non-zero, return its value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            raw = keeper_utils.send_4lw_cmd(cluster, node, cmd="lgif")
            parsed = _parse_lgif(raw)
            idx = int(parsed.get("last_snapshot_idx", "0"))
            if idx > 0:
                return idx
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(
        f"last_snapshot_idx did not advance on {node.name} within {timeout}s"
    )


def test_v8_snapshot_follower_recovery(started_cluster):
    """A follower that misses writes receives a V8 snapshot and converges."""
    wait_v8_nodes()

    # Identify the leader (highest priority = node1 via priority=3 in config).
    leader = keeper_utils.get_leader(started_cluster, ALL_V8_NODES)
    # Pick any follower to lag behind.
    follower = next(n for n in ALL_V8_NODES if n is not leader)

    leader_zk = get_fake_zk(leader.name)
    try:
        prefix = "/test_v8_multinode"
        leader_zk.ensure_path(prefix)
        # Write one node so the follower is in sync before we stop it.
        leader_zk.create(f"{prefix}/sync_anchor", b"anchor")
        # Sync so the follower has committed the anchor before we stop it.
        follower_zk = get_fake_zk(follower.name)
        try:
            follower_zk.sync(f"{prefix}/sync_anchor")
        finally:
            stop_zk(follower_zk)
    finally:
        pass  # keep leader_zk alive for subsequent writes

    # Stop the follower — it will lag.
    follower.stop_clickhouse(kill=True)

    try:
        # Write 12 more nodes (>snapshot_distance=10) → V8 snapshot is triggered.
        for i in range(12):
            leader_zk.create(f"{prefix}/node{i}", f"data{i}".encode())
    finally:
        stop_zk(leader_zk)

    # Restart the follower — the leader sends it the V8 snapshot.
    follower.start_clickhouse(20)
    keeper_utils.wait_until_connected(started_cluster, follower, timeout=60)

    # Poll lgif until last_snapshot_idx advances (the snapshot was applied).
    snap_idx = _get_last_snapshot_idx(follower, timeout=60)
    assert snap_idx > 0, (
        f"last_snapshot_idx must be non-zero after V8 snapshot apply; got {snap_idx}"
    )

    # Verify all written nodes are readable on the follower.
    recovered_zk = get_fake_zk(follower.name)
    try:
        for i in range(12):
            data, _ = recovered_zk.get(f"{prefix}/node{i}")
            assert data == f"data{i}".encode(), (
                f"Node {i} has unexpected data on follower after V8 snapshot restore: {data!r}"
            )
    finally:
        stop_zk(recovered_zk)

    # Cleanup.
    cleanup_zk = get_fake_zk(leader.name)
    try:
        for i in range(12):
            cleanup_zk.delete(f"{prefix}/node{i}")
        cleanup_zk.delete(f"{prefix}/sync_anchor")
        cleanup_zk.delete(prefix)
    except Exception:
        pass
    finally:
        stop_zk(cleanup_zk)
