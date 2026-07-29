#!/usr/bin/env python3

"""
Rolling upgrade of a 3-node Keeper cluster from a released version that writes
snapshot format `V6` to the current build, bumping `write_snapshot_version` to
`V8` as part of the same restart.

This is the shape of a real upgrade: one node at a time is stopped, gets both a
new binary and a new `write_snapshot_version`, and rejoins while the remaining
nodes still run the old version. The old version cannot read `V8` snapshots, so
the cluster must stay healthy purely by catching the restarted node up from the
log; the log settings in the configs are large enough for that.
"""

import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

# The last version that supports snapshot format V7 at most, i.e. it cannot read
# the V8 snapshots written by the upgraded nodes.
OLD_VERSION = "26.4"

OLD_SNAPSHOT_VERSION = 6
NEW_SNAPSHOT_VERSION = 8

SNAPSHOT_DIR = "/var/lib/clickhouse/coordination/snapshots"

cluster = ClickHouseCluster(__file__)

# Disable `with_remote_database_disk` as the test does not use the default Keeper.
nodes = [
    cluster.add_instance(
        f"node{server_id}",
        main_configs=[f"configs/enable_keeper{server_id}.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
        with_installed_binary=True,
        image="clickhouse/clickhouse-server",
        tag=OLD_VERSION,
    )
    for server_id in (1, 2, 3)
]
node1, node2, node3 = nodes


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def keeper_config_path(node):
    return f"/etc/clickhouse-server/config.d/enable_keeper{node.name[-1]}.xml"


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def stop_zk(zk):
    if zk is None:
        return
    try:
        zk.stop()
        zk.close()
    except Exception:
        pass


def snapshot_log_idx(snapshot_name):
    # `snapshot_<log_idx>.bin[.zstd]` in older versions,
    # `snapshot_<log_idx>_<random>.bin[.zstd]` in newer ones.
    return int(snapshot_name.split("_")[1].split(".")[0])


def list_snapshots(node):
    files = node.exec_in_container(
        ["bash", "-c", f"ls -1 {SNAPSHOT_DIR}"], user="root"
    ).split()
    return sorted(
        (name for name in files if name.startswith("snapshot_")), key=snapshot_log_idx
    )


def get_snapshot_format_version(node, snapshot_name):
    # The format version is the very first byte of the snapshot payload, and the
    # payload is ZSTD-compressed as a whole, which `file` detects from the
    # `.zstd` suffix. Read it with `clickhouse local` inside the container so
    # that no decompression tool has to be present in the image.
    query = (
        "SELECT reinterpretAsUInt8(substring(raw_blob, 1, 1)) "
        f"FROM file('{SNAPSHOT_DIR}/{snapshot_name}', 'RawBLOB')"
    )
    result = node.exec_in_container(
        ["bash", "-c", f'cd /tmp && clickhouse local --query "{query}"'], user="root"
    )
    return int(result.strip())


def create_snapshot(node, timeout=60.0):
    """Take a snapshot on `node` and return the format version it was written with."""
    deadline = time.monotonic() + timeout

    # `csnp` answers with the log index of the scheduled snapshot, or with an error
    # message if another snapshot is still in progress.
    while True:
        response = keeper_utils.send_4lw_cmd(cluster, node, "csnp").strip()
        if response.isdigit():
            log_idx = int(response)
            break
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"{node.name} refused to snapshot within {timeout}s: {response!r}"
            )
        time.sleep(0.2)

    while True:
        snapshots = [
            name for name in list_snapshots(node) if snapshot_log_idx(name) >= log_idx
        ]
        if snapshots:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"{node.name} did not write a snapshot for log index {log_idx} "
                f"within {timeout}s, snapshots on disk: {list_snapshots(node)}"
            )
        time.sleep(0.2)

    return get_snapshot_format_version(node, snapshots[-1])


def get_server_version(node):
    return node.query("SELECT version()").strip()


def write_batch(zk, prefix, count):
    for i in range(count):
        zk.create(f"{prefix}_{i}", f"{prefix}_{i}".encode())


def assert_batch_visible(prefix, count, timeout=60.0):
    """Every node must eventually see the whole batch, whatever version it runs."""
    zks = []
    try:
        for node in nodes:
            zk = get_fake_zk(node)
            zks.append(zk)
            deadline = time.monotonic() + timeout
            for i in range(count):
                path = f"{prefix}_{i}"
                while zk.exists(path) is None:
                    if time.monotonic() >= deadline:
                        raise AssertionError(
                            f"{path} did not appear on {node.name} within {timeout}s"
                        )
                    time.sleep(0.05)
                assert zk.get(path)[0] == path.encode()
    finally:
        for zk in zks:
            stop_zk(zk)


def test_rolling_upgrade_bumps_snapshot_version(started_cluster):
    keeper_utils.wait_nodes(cluster, nodes)

    for node in nodes:
        assert get_server_version(node).startswith(f"{OLD_VERSION}.")

    # Data written by the old version, snapshotted in the old format.
    node1_zk = None
    try:
        node1_zk = get_fake_zk(node1)
        write_batch(node1_zk, "/before_upgrade", 100)
    finally:
        stop_zk(node1_zk)

    assert_batch_visible("/before_upgrade", 100)

    for node in nodes:
        assert create_snapshot(node) == OLD_SNAPSHOT_VERSION

    # Rolling restart, least-preferred leader first, so that the leader stays on
    # the old version for as long as possible.
    for step, node in enumerate(reversed(nodes)):
        node.replace_in_config(
            keeper_config_path(node),
            f"<write_snapshot_version>{OLD_SNAPSHOT_VERSION}</write_snapshot_version>",
            f"<write_snapshot_version>{NEW_SNAPSHOT_VERSION}</write_snapshot_version>",
        )
        node.restart_with_latest_version()
        keeper_utils.wait_until_connected(cluster, node)

        assert not get_server_version(node).startswith(f"{OLD_VERSION}.")

        # The mixed-version cluster must keep accepting and replicating writes.
        prefix = f"/during_upgrade_{step}"
        zk = None
        try:
            zk = get_fake_zk(node)
            write_batch(zk, prefix, 50)
        finally:
            stop_zk(zk)

        assert_batch_visible(prefix, 50)
        assert_batch_visible("/before_upgrade", 100)

    # Everything is upgraded now, so new snapshots must be written as V8.
    node1_zk = None
    try:
        node1_zk = get_fake_zk(node1)
        write_batch(node1_zk, "/after_upgrade", 100)
    finally:
        stop_zk(node1_zk)

    assert_batch_visible("/after_upgrade", 100)

    for node in nodes:
        assert create_snapshot(node) == NEW_SNAPSHOT_VERSION

    # Restarting from a V8 snapshot must restore all the data, including what was
    # written while the cluster still ran the old version.
    for node in nodes:
        node.restart_clickhouse(stop_start_wait_sec=120, kill=True)
        keeper_utils.wait_until_connected(cluster, node)

    assert_batch_visible("/before_upgrade", 100)
    for step in range(len(nodes)):
        assert_batch_visible(f"/during_upgrade_{step}", 50)
    assert_batch_visible("/after_upgrade", 100)
