#!/usr/bin/env python3
import os
import threading
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.keeper_snapshot_utils import (
    fill_test_tree,
    cleanup_test_tree,
    verify_test_tree,
    get_kill_timestamp,
    get_received_snapshot_info,
    stop_zk,
    _query_text_log,
)


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper1.xml", "configs/text_log.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/keeper2.xml", "configs/text_log.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/keeper3.xml", "configs/text_log.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ALL_NODES = [node1, node2, node3]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _get_follower(cluster_instance, nodes, exclude=None):
    """Dynamically select a follower node, optionally excluding a specific node."""
    for n in nodes:
        if n == exclude:
            continue
        if keeper_utils.is_follower(cluster_instance, n):
            return n
    raise Exception("No suitable follower found")


def _reset_keeper_storage(cluster_instance, nodes):
    for node in nodes:
        node.stop_clickhouse(kill=True)

    for node in nodes:
        node.exec_in_container(
            [
                "rm",
                "-rf",
                "/var/lib/clickhouse/coordination/logs",
                "/var/lib/clickhouse/coordination/snapshots",
            ]
        )

    for node in nodes:
        node.exec_in_container(
            ["bash", "-c", node.clickhouse_start_command],
            user=str(os.getuid()),
            detach=True,
            use_cli=False,
        )

    deadline = time.monotonic() + 30
    while True:
        missing = [node.name for node in nodes if node.get_process_pid("clickhouse") is None]
        if not missing:
            break
        if time.monotonic() >= deadline:
            raise Exception(f"ClickHouse processes did not start on {missing}")
        time.sleep(0.1)

    for node in nodes:
        node.wait_start(120)

    keeper_utils.wait_nodes(cluster_instance, nodes)


def test_deterministic_rotation_race(started_cluster):
    """Validate snapshot pinning under deterministic snapshot rotation.

    The test forces snapshot creation while a stopped follower recovers. It
    verifies that unavailable requested snapshots are counted as
    `KeeperReadSnapshotDeferred`, snapshot reads do not report failures, and
    a received snapshot is unlinked only after the leader finishes reading it.

    Sequence:
      1. Stop the lagger.
      2. Fill the tree to seed initial snapshots.
      3. Capture event baselines before lagger restart.
      4. Start a bounded `csnp` pump and restart the lagger.
      5. Wait for the lagger to come back online.
      6a. Assert the pin-miss deferral log appears.
      6b. Assert `KeeperReadSnapshotDeferred` delta >= 1.
      7-8. Verify lagger recovery (snapshot received, tree match, sample
           rotation writes). Capture received snapshot log_idx for
           Phase 10.
      9. Assert `KeeperReadSnapshotFailed` delta == 0.
     10. Assert the deletion log for the received snapshot appears after the
         leader's last read log for that snapshot.
    """
    # --- Phase 0: Reset persisted Keeper state and select nodes ---
    _reset_keeper_storage(started_cluster, ALL_NODES)

    leader = keeper_utils.get_leader(started_cluster, ALL_NODES)
    lagger = _get_follower(started_cluster, ALL_NODES)

    prefix = "/test_det_rotation_race"
    cleanup_test_tree(started_cluster, leader, prefix)

    leader_zk = keeper_utils.get_fake_zk(started_cluster, leader.name)
    lagger_zk = keeper_utils.get_fake_zk(started_cluster, lagger.name)

    # Seed data before stopping the lagger.
    leader_zk.create(prefix, b"basedata")
    assert lagger_zk.get(prefix)[0] == b"basedata"
    stop_zk(lagger_zk)

    # --- Phase 1: Stop lagger, generate enough writes to trigger snapshots ---
    lagger_after_time = get_kill_timestamp(lagger)
    lagger.stop_clickhouse(kill=True)

    # Fill with enough entries that the resulting snapshots span multiple
    # chunks (snapshot_transfer_chunk_size=4096, random znodes don't compress).
    # 800 znodes of 1 KiB random → ~800 KiB snapshot → ~200 chunks per transfer,
    # giving ample time for rotation to land between chunks.
    fill_test_tree(leader_zk, prefix, count=800)

    # --- Phase 2: Scope timestamp for log assertions ---
    rotation_time = get_kill_timestamp(leader)

    rotation_prefix = f"{prefix}/rotation"
    leader_zk.ensure_path(rotation_prefix)

    # --- Phase 3: Prepare a bounded write + `csnp` pump ---
    stop_csnp = threading.Event()
    csnp_writes = 0
    csnp_lock = threading.Lock()
    max_csnp_writes = 300

    def _csnp_pump():
        nonlocal csnp_writes
        local_zk = keeper_utils.get_fake_zk(started_cluster, leader.name)
        try:
            idx = 0
            while not stop_csnp.is_set() and idx < max_csnp_writes:
                try:
                    for _ in range(3):
                        local_zk.create(
                            f"{rotation_prefix}/{idx}", os.urandom(2048)
                        )
                        idx += 1
                    keeper_utils.send_4lw_cmd(started_cluster, leader, "csnp")
                    with csnp_lock:
                        csnp_writes = idx
                except Exception:
                    time.sleep(0.02)
        finally:
            stop_zk(local_zk)

    csnp_thread = threading.Thread(target=_csnp_pump, daemon=True)

    def _event_value(event):
        raw = leader.query(f"SELECT value FROM system.events WHERE event = '{event}'").strip()
        return int(raw) if raw else 0

    # Capture read failures across the recovery-under-rotation window.
    # Any nonzero delta is asserted after recovery so late increments count.
    failed_baseline = _event_value("KeeperReadSnapshotFailed")
    # Separate counter for pin-miss deferrals (`getSnapshotPin == nullptr`).
    # The deterministic rotation should request at least one retired snapshot.
    deferred_baseline = _event_value("KeeperReadSnapshotDeferred")

    try:
        # --- Phase 4: Restart the lagger while rotations are flying ---
        csnp_thread.start()
        lagger.start_clickhouse(20)

        # --- Phase 5: Wait for the lagger to come back online ---
        keeper_utils.wait_until_connected(started_cluster, lagger, timeout=120)

        # Keep rotating briefly while snapshot chunks may still be flushing.
        time.sleep(3)
    finally:
        stop_csnp.set()
        csnp_thread.join(timeout=10)

    with csnp_lock:
        produced = csnp_writes
    assert produced >= 20, (
        f"csnp pump produced only {produced} writes "
        f"(expected >= 20 to drive rotation)."
    )

    # --- Phase 6: Verify the pin-miss deferral log appears ---
    pin_miss_lines = _query_text_log(
        leader,
        rotation_time,
        "Snapshot with last log index % is no longer available locally; declining transfer",
        timeout=15,
    )
    assert pin_miss_lines, (
        "Expected a pin-miss deferral log under csnp-forced rotation. "
        "Either the csnp pump did not rotate enough snapshots, the lagger "
        "recovered too fast, or the pin acquisition logic regressed."
    )

    # --- Phase 6b: Verify the pin-miss path fired ---
    # Under snapshots_to_keep=1 + csnp-forced rotation + a recovering lagger,
    # NuRaft's stale sync_ctx will request a retired snapshot, hitting
    # `getSnapshotPinUnlocked` returning nullptr. That path:
    #   * logs "Snapshot with last log index N is no longer available locally"
    #   * increments KeeperReadSnapshotDeferred
    # Assert on the counter because it is more robust than matching logs.
    deferred_post = _event_value("KeeperReadSnapshotDeferred")
    deferred_delta = deferred_post - deferred_baseline
    assert deferred_delta >= 1, (
        f"KeeperReadSnapshotDeferred delta = {deferred_delta} "
        f"(baseline {deferred_baseline} -> {deferred_post}); expected >= 1. "
        f"The pin-miss convergence path did NOT fire during the rotation race. "
        f"Either the csnp pump did not rotate enough snapshots, the lagger "
        f"recovered too fast, or the pin acquisition logic regressed."
    )

    # --- Phase 7-8: Verify the lagger received a snapshot and recovered ---
    received = get_received_snapshot_info(lagger, lagger_after_time, timeout=30)
    assert received is not None, "Lagger did not receive a snapshot after recovery"
    received_log_idx, _received_chunks, _received_bytes = received

    lagger_zk = keeper_utils.get_fake_zk(started_cluster, lagger.name)

    verify_test_tree(leader_zk, lagger_zk, prefix)

    # Verify a sample of forced writes reached the lagger
    sample_indices = range(0, produced, max(1, produced // 20))
    for i in sample_indices:
        assert lagger_zk.exists(f"{rotation_prefix}/{i}") is not None, (
            f"Rotation write {rotation_prefix}/{i} missing on lagger"
        )

    # --- Phase 9: `KeeperReadSnapshotFailed` delta must be 0 ---
    # Pin-miss deferrals are counted by `KeeperReadSnapshotDeferred`, so this
    # counter is reserved for loader-init, chunk-read, and buffer-allocation failures.
    failed_post = _event_value("KeeperReadSnapshotFailed")
    failed_delta = failed_post - failed_baseline
    MAX_RESIDUAL_FAILED = 0
    assert failed_delta <= MAX_RESIDUAL_FAILED, (
        f"KeeperReadSnapshotFailed delta = {failed_delta} "
        f"(baseline {failed_baseline} -> {failed_post}); expected <= "
        f"{MAX_RESIDUAL_FAILED}. The pin-miss path is counted separately "
        f"as `KeeperReadSnapshotDeferred` (Phase 6b "
        f"delta was {deferred_delta}), so a nonzero KeeperReadSnapshotFailed "
        f"delta indicates a real I/O / loader-init failure on the leader's "
        f"read path - which is a regression worth investigating."
    )

    # --- Phase 10 (deferred-deletion ordering): timestamp proof ---
    # The leader logs reads while the pin is held. The deletion log must appear
    # after the last read log for the snapshot the lagger installed.
    last_read_lines = _query_text_log(
        leader,
        rotation_time,
        f"Reading snapshot {received_log_idx} obj_id %",
        timeout=15,
    )
    deletion_lines = _query_text_log(
        leader,
        rotation_time,
        f"Removed outdated snapshot {received_log_idx} at path %",
        timeout=30,
    )
    assert deletion_lines, (
        f"Expected 'Removed outdated snapshot {received_log_idx}' on the "
        f"leader. snapshots_to_keep=1 + csnp pump must retire the received "
        f"snapshot during the test window. If this fires, the pin-side "
        f"deferral logic may be leaking refs."
    )
    # Do not skip this check on empty logs: a mid-test leader change makes the
    # ordering proof invalid and should fail loudly.
    assert last_read_lines, (
        f"Expected 'Reading snapshot {received_log_idx} obj_id' on the "
        f"original leader. An empty list most likely means a leader change "
        f"happened mid-test and the lagger received the snapshot from a "
        f"different node - in which case the timestamp-ordering proof below "
        f"cannot be evaluated against this `leader` variable. Re-pick the "
        f"leader before Phase 10 if this fires."
    )
    # Cross-check timestamp ordering by querying both with the same WHERE
    # clause to obtain event_time_microseconds explicitly.
    last_read_ts = leader.query(
        f"SELECT max(event_time_microseconds) FROM system.text_log "
        f"WHERE event_time_microseconds > '{rotation_time}' "
        f"AND message LIKE 'Reading snapshot {received_log_idx} obj_id %'"
    ).strip()
    deletion_ts = leader.query(
        f"SELECT min(event_time_microseconds) FROM system.text_log "
        f"WHERE event_time_microseconds > '{rotation_time}' "
        f"AND message LIKE 'Removed outdated snapshot {received_log_idx} at path %'"
    ).strip()
    assert last_read_ts and deletion_ts, (
        f"Could not obtain both timestamps: last_read_ts={last_read_ts!r}, "
        f"deletion_ts={deletion_ts!r}"
    )
    assert deletion_ts >= last_read_ts, (
        f"Deferred-deletion ordering violation for snapshot "
        f"{received_log_idx}: deletion at {deletion_ts} fired BEFORE "
        f"last leader read at {last_read_ts}. The pin should have kept "
        f"the file alive until the transfer completed."
    )

    stop_zk(leader_zk)
    stop_zk(lagger_zk)
    cleanup_test_tree(started_cluster, leader, prefix)


def test_stable_recovery_under_write_load(started_cluster):
    """Verify that the follower recovers when snapshots rotate under write load.

    This test does NOT use a failpoint. It stops the lagger, writes enough to
    trigger an initial snapshot, then restarts the lagger and runs a bounded
    concurrent background writer to increase the chance of natural snapshot
    rotation during the transfer.

    It asserts only that the follower recovers all data, making it a stable
    regression test for CI. The text-log check below is informational because
    rotation overlap is timing-dependent.
    """
    _reset_keeper_storage(started_cluster, ALL_NODES)

    leader = keeper_utils.get_leader(started_cluster, ALL_NODES)
    lagger = _get_follower(started_cluster, ALL_NODES)

    prefix = "/test_load_rotation_recovery"
    cleanup_test_tree(started_cluster, leader, prefix)

    leader_zk = keeper_utils.get_fake_zk(started_cluster, leader.name)
    lagger_zk = keeper_utils.get_fake_zk(started_cluster, lagger.name)

    leader_zk.create(prefix, b"basedata")
    assert lagger_zk.get(prefix)[0] == b"basedata"
    stop_zk(lagger_zk)

    lagger_after_time = get_kill_timestamp(lagger)
    lagger.stop_clickhouse(kill=True)

    fill_test_tree(leader_zk, prefix)

    leader_after_time = get_kill_timestamp(leader)

    TARGET_BG_WRITES = 200
    bg_prefix = f"{prefix}/bg"
    leader_zk.ensure_path(bg_prefix)
    bg_paths = []
    bg_errors = []
    bg_lock = threading.Lock()
    stop_writer = threading.Event()

    def _background_writer():
        idx = 0
        while not stop_writer.is_set() and idx < TARGET_BG_WRITES:
            try:
                path = f"{bg_prefix}/{idx}"
                leader_zk.create(path, os.urandom(1024))
                with bg_lock:
                    bg_paths.append(path)
                idx += 1
            except Exception as e:
                with bg_lock:
                    bg_errors.append(str(e))
            # No sleep: maximum churn so snapshot_distance=50 trips repeatedly
            # during the lagger's recovery window. Rotation overlapping with
            # snapshot install is the property the test is trying to exercise.

    writer_thread = threading.Thread(target=_background_writer, daemon=True)
    writer_thread.start()

    try:
        lagger.start_clickhouse(20)
        keeper_utils.wait_until_connected(started_cluster, lagger, timeout=60)
        # Keep the writer going past the moment the lagger comes back so
        # snapshot rotation actually lands DURING the install window.
        writer_thread.join(timeout=30)
    finally:
        stop_writer.set()
        writer_thread.join(timeout=10)

    with bg_lock:
        paths_to_check = list(bg_paths)
        errors_sample = list(bg_errors[:5])
    # Smoke check that the writer did its job; absolute count is environment
    # dependent (TSan / asan slow the runner materially).
    assert len(paths_to_check) >= 10, (
        f"Background writer created only {len(paths_to_check)} znodes "
        f"(expected >= 10 for meaningful load). "
        f"First errors: {errors_sample}"
    )

    received = get_received_snapshot_info(lagger, lagger_after_time, timeout=30)
    assert received is not None, "Lagger did not receive a snapshot"

    lagger_zk = keeper_utils.get_fake_zk(started_cluster, lagger.name)

    verify_test_tree(leader_zk, lagger_zk, prefix)

    for path in paths_to_check:
        assert lagger_zk.exists(path) is not None, (
            f"Background write {path} missing on lagger after recovery"
        )

    pin_miss_lines = _query_text_log(
        leader,
        leader_after_time,
        "Snapshot with last log index % is no longer available locally; declining transfer",
        timeout=5,
    )
    if pin_miss_lines:
        print(
            f"INFO: Pin-miss deferral observed during load test: "
            f"{pin_miss_lines[0]}"
        )
    else:
        print(
            "INFO: Pin-miss deferral was not observed during this run "
            "(expected -- overlap is timing-dependent)"
        )

    stop_zk(leader_zk)
    stop_zk(lagger_zk)
    cleanup_test_tree(started_cluster, leader, prefix)
