#!/usr/bin/env python3
"""Integration test for Keeper chunked snapshot format with ZSTD compression.

Verifies that a single-node Keeper can:
- Start with `write_snapshot_version=9` and `use_compression=true`.
- Trigger a chunked snapshot by writing enough entries to exceed `snapshot_distance`.
- Restart and successfully restore state from the chunked snapshot.

The configuration uses `snapshot_distance=5` so that 6 writes guarantee a
snapshot is taken without long waiting.
"""

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Node uses the base Keeper config, the compression overlay, and the chunked settings overlay.
node_chunked = cluster.add_instance(
    "node_chunked",
    main_configs=[
        "configs/keeper_chunked_base.xml",
        "configs/keeper_with_compression_chunked.xml",
        "configs/keeper_chunked_settings.xml",
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


def test_chunked_snapshot_start_and_restart(started_cluster):
    """Keeper with write_snapshot_version=9 and ZSTD compression starts, persists
    data across a chunked snapshot, and restores state after a restart.

    The test forces an actual chunked snapshot to be created (not just WAL replay) by:
    1. Writing 6 entries (> snapshot_distance=5) to trigger snapshot creation.
    2. Waiting for the "Created persistent snapshot" log line to confirm the snapshot
       was written before restarting.
    3. Asserting all written data survives the restart, proving the chunked read path ran.
    """
    keeper_utils.wait_nodes(cluster, [node_chunked])

    prefix = "/test_chunked_compression"

    # Capture the current log position so wait_for_log_line only sees new lines.
    log_anchor = node_chunked.count_log_lines()

    zk = keeper_utils.get_fake_zk(cluster, node_chunked.name, timeout=30.0)
    try:
        # Write 6 entries (ensure_path + 5 creates > snapshot_distance=5),
        # guaranteeing a chunked snapshot is triggered before the restart.
        zk.ensure_path(prefix)
        for i in range(5):
            zk.create(f"{prefix}/node{i}", f"value{i}".encode())
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass

    # Wait for Keeper to confirm it wrote the chunked snapshot to disk.
    # This verifies the chunked write path ran, not just WAL accumulation.
    node_chunked.wait_for_log_line(
        "Created persistent snapshot",
        timeout=30,
        look_behind_lines=f"+{log_anchor}",
    )

    # Restart so the node loads exclusively from the chunked snapshot on-disk.
    node_chunked.restart_clickhouse(kill=True)
    keeper_utils.wait_nodes(cluster, [node_chunked])

    # Verify all written nodes are present, proving the chunked read path restored state.
    zk2 = keeper_utils.get_fake_zk(cluster, node_chunked.name, timeout=30.0)
    try:
        for i in range(5):
            data, _ = zk2.get(f"{prefix}/node{i}")
            assert data == f"value{i}".encode(), (
                f"Node {i} has wrong data after chunked snapshot restore: {data!r}"
            )
    finally:
        try:
            zk2.stop()
            zk2.close()
        except Exception:
            pass
