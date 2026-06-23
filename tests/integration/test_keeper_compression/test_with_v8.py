#!/usr/bin/env python3
"""Integration test for Keeper V8 snapshot format with ZSTD compression.

Verifies that a single-node Keeper can:
- Start with `write_snapshot_version=8` and `use_compression=true`.
- Trigger a V8 snapshot by writing enough entries to exceed `snapshot_distance`.
- Restart and successfully restore state from the V8 snapshot.

The configuration uses `snapshot_distance=5` so that 6 writes guarantee a
snapshot is taken without long waiting.
"""

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Node uses the base Keeper config, the compression overlay, and the V8 overlay.
node_v8 = cluster.add_instance(
    "node_v8",
    main_configs=[
        "configs/keeper_v8_base.xml",
        "configs/keeper_with_compression_v8.xml",
        "configs/keeper_v8_settings.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_v8_snapshot_start_and_restart(started_cluster):
    """Keeper with write_snapshot_version=8 and ZSTD compression starts, persists
    data across a V8 snapshot, and restores state after a restart.

    The test forces an actual V8 snapshot to be created (not just WAL replay) by:
    1. Writing 6 entries (> snapshot_distance=5) to trigger snapshot creation.
    2. Waiting for the "Created persistent snapshot" log line to confirm the snapshot
       was written before restarting.
    3. Asserting all written data survives the restart, proving the V8 read path ran.
    """
    keeper_utils.wait_nodes(cluster, [node_v8])

    prefix = "/test_v8_compression"

    # Capture the current log position so wait_for_log_line only sees new lines.
    log_anchor = node_v8.count_log_lines()

    zk = keeper_utils.get_fake_zk(cluster, node_v8.name, timeout=30.0)
    try:
        # Write 6 entries (ensure_path + 5 creates > snapshot_distance=5),
        # guaranteeing a V8 snapshot is triggered before the restart.
        zk.ensure_path(prefix)
        for i in range(5):
            zk.create(f"{prefix}/node{i}", f"value{i}".encode())
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass

    # Wait for Keeper to confirm it wrote the V8 snapshot to disk.
    # This verifies the V8 write path ran, not just WAL accumulation.
    node_v8.wait_for_log_line(
        "Created persistent snapshot",
        timeout=30,
        look_behind_lines=f"+{log_anchor}",
    )

    # Restart so the node loads exclusively from the V8 snapshot on-disk.
    node_v8.restart_clickhouse(kill=True)
    keeper_utils.wait_nodes(cluster, [node_v8])

    # Verify all written nodes are present, proving the V8 read path restored state.
    zk2 = keeper_utils.get_fake_zk(cluster, node_v8.name, timeout=30.0)
    try:
        for i in range(5):
            data, _ = zk2.get(f"{prefix}/node{i}")
            assert data == f"value{i}".encode(), (
                f"Node {i} has wrong data after V8 snapshot restore: {data!r}"
            )
    finally:
        try:
            zk2.stop()
            zk2.close()
        except Exception:
            pass
