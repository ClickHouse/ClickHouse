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
    lagger_zk.sync(prefix)
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
