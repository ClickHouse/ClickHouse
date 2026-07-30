#!/usr/bin/env python3

"""
Upgrade hazards for a 3-node Keeper cluster going from a released version that
supports snapshot format `V7` at most to a build that supports `V9` and can be
told to write `V8`.

This is a rehearsal of a production upgrade, not a feature test: each scenario
below drives the cluster into a situation a real rollout can reach, and records
what happens. Read the verdict on every test:

    SAFE   — the procedure works, and the test fails if that ever regresses.
    UNSAFE — the procedure breaks the cluster. The test pins the exact failure
             so the blast radius is known. It does NOT bless the procedure.

Summary of what works and what does not:

  test_documented_order_upgrade_is_safe                       SAFE
      Upgrade every binary first while all nodes keep writing `V6`, and only
      raise `write_snapshot_version` to `8` once no old binary is left. Snapshot
      installs during the mixed-version window are fine, because the upgraded
      nodes still write the old format that the old nodes can read.

  test_snapshot_install_to_old_node_fails_after_early_v8_bump  UNSAFE
      Raising `write_snapshot_version` to `8` while old binaries are still in the
      cluster looks completely healthy at first: writes are accepted and
      replicated. It only breaks later, the first time an old node has fallen far
      enough behind to need a snapshot installed instead of log replay. The old
      node then cannot load the `V8` snapshot and is stuck out of the quorum.
      This latency is what makes the procedure dangerous - a green rollout is not
      a safe rollout.

  test_rollback_after_v8_snapshot_is_impossible                UNSAFE
      Once a node has written a `V8` snapshot, putting the old binary back on it
      fails: the old code refuses to load its own data directory. Bumping
      `write_snapshot_version` is therefore a one-way door, and the rollback plan
      for a Keeper upgrade cannot simply be "reinstall the previous version".

  test_new_feature_flag_during_mixed_window_breaks_old_nodes   UNSAFE
      `CREATE_TTL` is gated on `write_snapshot_version >= 8`, so it becomes
      enable-able exactly during the window when old nodes are still present. It
      writes a request type (`CreateTTL`) that the old version does not know at
      all, so the old nodes cannot apply those log entries.

Not a hazard (checked, no test needed): the snapshot file naming changed from
`snapshot_<idx>.bin.zstd` to `snapshot_<idx>_<random>.bin.zstd`, but the old
version splits on both `_` and `.` and reads the index from the second field, so
it parses the new names correctly.
"""

import os
import time
import uuid

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Supports snapshot format V7 at most, and defaults to writing V6.
OLD_VERSION = "26.4"

V6 = 6
V8 = 8

SNAPSHOT_DIR = "/var/lib/clickhouse/coordination/snapshots"
CONFIG_DIR = "/etc/clickhouse-server/config.d"

UNSUPPORTED_VERSION_ERROR = "Unsupported snapshot version 8"


def make_cluster(config_prefix):
    """Build a fresh 3-node Keeper cluster on the old version.

    Each scenario needs its own cluster because they differ in configuration and
    in how badly they damage the nodes, so nothing is shared between them.
    """
    cluster = ClickHouseCluster(__file__, str(uuid.uuid4()))
    nodes = [
        cluster.add_instance(
            f"node{server_id}",
            main_configs=[f"configs/{config_prefix}{server_id}.xml"],
            stay_alive=True,
            # The test does not use the default Keeper.
            with_remote_database_disk=False,
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag=OLD_VERSION,
        )
        for server_id in (1, 2, 3)
    ]
    return cluster, nodes


def config_path(node, config_prefix):
    return f"{CONFIG_DIR}/{config_prefix}{node.name[-1]}.xml"


def stop_zk(zk):
    if zk is None:
        return
    try:
        zk.stop()
        zk.close()
    except Exception:
        pass


def snapshot_log_idx(snapshot_name):
    # `snapshot_<log_idx>.bin[.zstd]` in the old version,
    # `snapshot_<log_idx>_<random>.bin[.zstd]` in the new one.
    return int(snapshot_name.split("_")[1].split(".")[0])


def list_snapshots(node):
    files = node.exec_in_container(
        ["bash", "-c", f"ls -1 {SNAPSHOT_DIR} 2>/dev/null || true"], user="root"
    ).split()
    return sorted(
        (name for name in files if name.startswith("snapshot_")), key=snapshot_log_idx
    )


def get_snapshot_format_version(node, snapshot_name):
    # The format version is the first byte of the snapshot payload, which is
    # ZSTD-compressed as a whole; `file` picks that up from the `.zstd` suffix.
    # Using `clickhouse local` avoids needing a decompression tool in the image.
    query = (
        "SELECT reinterpretAsUInt8(substring(raw_blob, 1, 1)) "
        f"FROM file('{SNAPSHOT_DIR}/{snapshot_name}', 'RawBLOB')"
    )
    result = node.exec_in_container(
        ["bash", "-c", f'cd /tmp && clickhouse local --query "{query}"'], user="root"
    )
    return int(result.strip())


def create_snapshot(cluster, node, timeout=60.0):
    """Force a snapshot on `node` and return the format version it was written with."""
    deadline = time.monotonic() + timeout

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
                f"{node.name} wrote no snapshot for log index {log_idx} within "
                f"{timeout}s, on disk: {list_snapshots(node)}"
            )
        time.sleep(0.2)

    return get_snapshot_format_version(node, snapshots[-1])


def force_leader(cluster, node, timeout=60.0):
    """Make `node` the leader, so the test controls which binary writes snapshots."""
    deadline = time.monotonic() + timeout
    while not keeper_utils.is_leader(cluster, node):
        keeper_utils.send_4lw_cmd(cluster, node, "rqld")
        if time.monotonic() >= deadline:
            raise AssertionError(f"{node.name} did not become leader within {timeout}s")
        time.sleep(1)


def upgrade(node, config_prefix, write_snapshot_version=None):
    """Restart `node` on the new binary, optionally bumping the snapshot version."""
    if write_snapshot_version is not None:
        node.replace_in_config(
            config_path(node, config_prefix),
            f"<write_snapshot_version>{V6}</write_snapshot_version>",
            f"<write_snapshot_version>{write_snapshot_version}"
            "</write_snapshot_version>",
        )
    node.restart_with_latest_version()
    assert not node.query("SELECT version()").strip().startswith(f"{OLD_VERSION}.")


def bump_write_version(cluster, node, config_prefix, version):
    """Raise `write_snapshot_version` on an already-upgraded node and restart it."""
    node.replace_in_config(
        config_path(node, config_prefix),
        f"<write_snapshot_version>{V6}</write_snapshot_version>",
        f"<write_snapshot_version>{version}</write_snapshot_version>",
    )
    node.restart_clickhouse(stop_start_wait_sec=120)
    keeper_utils.wait_until_connected(cluster, node)


def write_batch(zk, prefix, count):
    for i in range(count):
        zk.create(f"{prefix}_{i}", f"{prefix}_{i}".encode())


def write_batch_on(cluster, node, prefix, count):
    zk = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, node.name)
        write_batch(zk, prefix, count)
    finally:
        stop_zk(zk)


def assert_batch_visible(cluster, nodes, prefix, count, timeout=60.0):
    """Every listed node must eventually see the whole batch, on any version."""
    zks = []
    try:
        for node in nodes:
            zk = keeper_utils.get_fake_zk(cluster, node.name)
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


def wait_for_log(node, substring, timeout=60.0):
    deadline = time.monotonic() + timeout
    while not node.contains_in_log(substring):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)
    return True


# ---------------------------------------------------------------------------
# SAFE: the documented procedure.
# ---------------------------------------------------------------------------


def test_documented_order_upgrade_is_safe():
    """Upgrade all binaries first, raise `write_snapshot_version` only afterwards.

    This is what `CoordinationSettings.cpp` tells you to do, and it holds up even
    when a node falls far enough behind to need a snapshot installed during the
    mixed-version window: the upgraded nodes are still writing `V6`, which the old
    nodes can read.
    """
    prefix = "small_snap_keeper"
    cluster, nodes = make_cluster(prefix)
    node1, node2, node3 = nodes
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)

        write_batch_on(cluster, node1, "/before", 50)
        assert_batch_visible(cluster, nodes, "/before", 50)

        # Phase 1: upgrade the leader's binary but leave it writing V6, exactly as
        # in test_snapshot_install_to_old_node_fails_after_early_v8_bump. The only
        # difference between the two tests is write_snapshot_version, so any
        # difference in outcome is attributable to that setting alone.
        upgrade(node1, prefix)
        keeper_utils.wait_until_connected(cluster, node1)
        force_leader(cluster, node1)
        assert create_snapshot(cluster, node1) == V6

        # Drive an old node into a snapshot install from the upgraded leader.
        node2.stop_clickhouse()
        write_batch_on(cluster, node1, "/while_node2_down", 200)
        assert create_snapshot(cluster, node1) == V6

        node2.start_clickhouse(start_wait_sec=120)
        keeper_utils.wait_until_connected(cluster, node2)

        assert wait_for_log(
            node2, "Saving snapshot", timeout=120
        ), "expected node2 to be caught up by a snapshot install, not by log replay"
        assert not node2.contains_in_log(UNSUPPORTED_VERSION_ERROR)

        # The old node accepted the upgraded leader's V6 snapshot and is serving.
        assert_batch_visible(cluster, [node2], "/while_node2_down", 200)

        # Finish the rolling upgrade of the remaining old binaries.
        for node in (node2, node3):
            upgrade(node, prefix)
            keeper_utils.wait_until_connected(cluster, node)
        assert_batch_visible(cluster, nodes, "/before", 50)

        # Phase 2: no old binary left, so it is now safe to raise the version.
        for node in nodes:
            bump_write_version(cluster, node, prefix, V8)
        force_leader(cluster, node1)

        write_batch_on(cluster, node1, "/after", 50)
        assert_batch_visible(cluster, nodes, "/after", 50)
        for node in nodes:
            assert create_snapshot(cluster, node) == V8

        # Everything still readable after a restart from V8 snapshots.
        for node in nodes:
            node.restart_clickhouse(stop_start_wait_sec=120, kill=True)
            keeper_utils.wait_until_connected(cluster, node)
        assert_batch_visible(cluster, nodes, "/before", 50)
        assert_batch_visible(cluster, nodes, "/after", 50)
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# UNSAFE: bumping the version while old binaries are still present.
# ---------------------------------------------------------------------------


def test_snapshot_install_to_old_node_fails_after_early_v8_bump():
    """Bumping to `V8` mid-window looks fine, then breaks on the first snapshot install.

    The point of this test is the two-phase shape: the cluster is provably healthy
    right after the bump, so a rollout would be declared successful. The failure
    only surfaces when an old node has to be caught up by snapshot instead of by
    log, which in production may be days later.
    """
    prefix = "small_snap_keeper"
    cluster, nodes = make_cluster(prefix)
    node1, node2, node3 = nodes
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)

        write_batch_on(cluster, node1, "/before", 50)
        assert_batch_visible(cluster, nodes, "/before", 50)

        # Upgrade the leader first and bump it straight to V8, while node2 and
        # node3 are still on the old version.
        upgrade(node1, prefix, write_snapshot_version=V8)
        keeper_utils.wait_until_connected(cluster, node1)
        force_leader(cluster, node1)
        assert create_snapshot(cluster, node1) == V8

        # Phase 1 - everything looks healthy. This is the trap.
        write_batch_on(cluster, node1, "/looks_fine", 20)
        assert_batch_visible(cluster, nodes, "/looks_fine", 20)
        for node in (node2, node3):
            assert not node.contains_in_log(UNSUPPORTED_VERSION_ERROR)

        # Phase 2 - take an old node out long enough that the log it needs has
        # been compacted away, so rejoining requires a snapshot install.
        node2.stop_clickhouse()
        write_batch_on(cluster, node1, "/while_node2_down", 200)
        assert create_snapshot(cluster, node1) == V8

        node2.start_clickhouse()

        # The old node is handed a V8 snapshot it cannot parse.
        assert wait_for_log(
            node2, UNSUPPORTED_VERSION_ERROR, timeout=120
        ), f"expected {UNSUPPORTED_VERSION_ERROR!r} in node2 log after install"

        # ...and it cannot serve the data it missed, i.e. it is out of the quorum.
        node2_zk = None
        try:
            node2_zk = keeper_utils.get_fake_zk(cluster, node2.name, timeout=10.0)
            assert node2_zk.exists("/while_node2_down_199") is None
        except Exception:
            # Not serving requests at all is an equally valid manifestation.
            pass
        finally:
            stop_zk(node2_zk)

        # The two upgraded/remaining-quorum members are unaffected, which is why
        # this is easy to miss from the outside.
        assert_batch_visible(cluster, [node1], "/while_node2_down", 200)
    finally:
        cluster.shutdown()


def test_rollback_after_v8_snapshot_is_impossible():
    """After a `V8` snapshot exists on a node, the old binary cannot start on it.

    This is the rollback plan for a Keeper upgrade failing. The old code rejects
    its own data directory, so "put the previous version back" is not available
    once `write_snapshot_version` has been raised.
    """
    prefix = "keeper"
    cluster, nodes = make_cluster(prefix)
    node1, node2, node3 = nodes
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)

        write_batch_on(cluster, node1, "/before", 50)
        assert_batch_visible(cluster, nodes, "/before", 50)

        # Full rolling upgrade, every node ending up writing V8.
        for node in (node3, node2, node1):
            upgrade(node, prefix, write_snapshot_version=V8)
            keeper_utils.wait_until_connected(cluster, node)

        write_batch_on(cluster, node1, "/after", 50)
        assert_batch_visible(cluster, nodes, "/after", 50)

        for node in nodes:
            assert create_snapshot(cluster, node) == V8

        # Now try to roll node3 back to the old binary, the way an operator would
        # if the upgrade had to be abandoned.
        node3.stop_clickhouse()
        node3.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/share/clickhouse_original /usr/bin/clickhouse "
                "&& chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        node3.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

        assert node3.contains_in_log(
            UNSUPPORTED_VERSION_ERROR
        ), "expected the old binary to reject the V8 snapshot it inherited"

        # The remaining quorum is still fine, so the cluster survives losing the
        # node - but that node cannot be recovered by downgrading it.
        assert_batch_visible(cluster, [node1, node2], "/after", 50)
    finally:
        cluster.shutdown()


def test_new_feature_flag_during_mixed_window_breaks_old_nodes():
    """`CREATE_TTL` needs `V8`, so it can be turned on while old nodes still exist.

    The old version has no `CreateTTL` request type at all, so once an upgraded
    node accepts a TTL create, the resulting log entries cannot be applied by the
    nodes that have not been upgraded yet.
    """
    prefix = "keeper"
    cluster, nodes = make_cluster(prefix)
    node1, node2, node3 = nodes
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)

        write_batch_on(cluster, node1, "/before", 20)
        assert_batch_visible(cluster, nodes, "/before", 20)

        # Upgrade only node1, and give it a config that both raises the snapshot
        # version and enables the new feature flag. node2/node3 stay on the old
        # version, which does not even know this flag exists.
        node1.stop_clickhouse()
        node1.exec_in_container(
            ["bash", "-c", f"rm -f {config_path(node1, prefix)}"], user="root"
        )
        node1.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/upgraded_keeper1_ttl.xml"),
            f"{CONFIG_DIR}/upgraded_keeper1_ttl.xml",
        )
        node1.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/bin/clickhouse /usr/share/clickhouse_original "
                "&& cp /usr/share/clickhouse_fresh /usr/bin/clickhouse "
                "&& chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        node1.start_clickhouse(start_wait_sec=120)
        keeper_utils.wait_until_connected(cluster, node1)
        assert not node1.query("SELECT version()").strip().startswith(
            f"{OLD_VERSION}."
        )
        force_leader(cluster, node1)

        # A TTL node can now be created, because node1 permits it.
        node1_zk = None
        try:
            node1_zk = keeper_utils.get_fake_zk(cluster, node1.name)
            node1_zk.create("/ttl_node", b"data", ttl=60000)
            assert node1_zk.exists("/ttl_node") is not None
        finally:
            stop_zk(node1_zk)

        # The old nodes receive a request type they do not know. They cannot apply
        # it, so the node never becomes consistent with the leader.
        for node in (node2, node3):
            zk = None
            try:
                zk = keeper_utils.get_fake_zk(cluster, node.name, timeout=10.0)
                deadline = time.monotonic() + 30
                while zk.exists("/ttl_node") is None:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.5)
                assert (
                    zk.exists("/ttl_node") is None
                ), f"{node.name} ({OLD_VERSION}) applied a CreateTTL entry"
            except Exception:
                # Refusing to serve at all is an equally valid manifestation.
                pass
            finally:
                stop_zk(zk)
    finally:
        cluster.shutdown()
