#!/usr/bin/env python3

"""
Upgrade hazards for a 3-node Keeper cluster going from a released version that
supports snapshot format `V7` at most to a build that supports `V9` and can be
told to write `V8`.

This is a rehearsal of a production upgrade, not a feature test. The rollout
being rehearsed upgrades followers first and the leader last, which matters a
great deal: only the leader ever ships a snapshot to a peer
(`create_sync_snapshot_req` in `contrib/NuRaft/src/handle_snapshot_sync.cxx`
takes a peer), so keeping the leader on the old binary until the end means every
snapshot handed out during the mixed-version window is one the old nodes can
read. Read the verdict on every test:

    SAFE        — the procedure works, and the test fails if that regresses.
    RECOVERABLE — the procedure is disrupted but heals once the rollout
                  completes. The test fails if it does not heal, or if anything
                  is lost along the way.
    UNSAFE      — the procedure breaks the cluster with no way back. The test
                  pins the exact failure so the blast radius is known. It does
                  NOT bless the procedure.

Summary of what works and what does not:

  test_documented_order_upgrade_is_safe                       SAFE
      Upgrade every binary first, followers before the leader, while all nodes
      keep writing `V6`, and raise `write_snapshot_version` to `8` only once no
      old binary is left. A still-old follower that falls far enough behind to
      need a snapshot installed is caught up correctly, because the leader
      shipping that snapshot is still writing the old format.

  test_old_node_broken_by_v8_snapshot_recovers_when_upgraded   RECOVERABLE
      The residual risk of the followers-first order: if the still-old leader is
      lost involuntarily part-way through, leadership moves to an already-upgraded
      node writing `V8`, and the old node coming back is handed a snapshot it
      cannot read, so it aborts and keeps aborting - restarting is what
      re-triggers it. Because every node is upgraded eventually, what matters is
      that rolling that node forward clears it completely: it rejoins, serves
      every znode written while it was down, and takes part in snapshots again.
      The test fails if recovery does not happen or if anything is lost. Note the
      only valid intervention is forward; putting the old binary back does not
      work, for the separate reason below.

  test_rollback_after_v8_snapshot_is_impossible                UNSAFE
      Once a node has written a `V8` snapshot, putting the old binary back on it
      fails: the old code refuses to load its own data directory. Bumping
      `write_snapshot_version` is therefore a one-way door, and the rollback plan
      for a Keeper upgrade cannot simply be "reinstall the previous version".
      Upgrade order does not help with this one - the first follower to be
      upgraded loses the ability to go back, before anything has been learned
      about whether the upgrade is healthy.

Deliberately not covered here:

  - Upgrading the leader first, or raising `write_snapshot_version` on a node
    while old binaries are still present. Both make an upgraded node hand a `V8`
    snapshot to an old one, which fails in `readMetadata`, but neither is part of
    the rollout being rehearsed.
  - Enabling a new feature flag such as `CREATE_TTL`, which is gated on
    `write_snapshot_version >= 8`, part-way through the window.

Not a hazard (checked, no test needed): the snapshot file naming changed from
`snapshot_<idx>.bin.zstd` to `snapshot_<idx>_<random>.bin.zstd`, but the old
version splits on both `_` and `.` and reads the index from the second field, so
it parses the new names correctly.
"""

import time
import uuid

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

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


def upgrade_stopped_node(node, config_prefix, write_snapshot_version=None):
    """Put the new binary on a node whose process is not running.

    `restart_with_latest_version` cannot be used here because it starts by
    `pkill`-ing ClickHouse, which fails when the process has already died on its
    own - which is exactly the situation this helper exists for.
    """
    if write_snapshot_version is not None:
        node.replace_in_config(
            config_path(node, config_prefix),
            f"<write_snapshot_version>{V6}</write_snapshot_version>",
            f"<write_snapshot_version>{write_snapshot_version}"
            "</write_snapshot_version>",
        )
    node.exec_in_container(["bash", "-c", "pkill -9 clickhouse || true"], user="root")
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp /usr/bin/clickhouse /usr/share/clickhouse_original "
            "&& cp /usr/share/clickhouse_fresh /usr/bin/clickhouse "
            "&& chmod 777 /usr/bin/clickhouse",
        ],
        user="root",
    )
    node.start_clickhouse(start_wait_sec=180)
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

        # Phase 1: followers first, leader last, every node still writing V6.
        # Because only the leader ever ships a snapshot to a peer, keeping the
        # leader on the old binary until the end means the snapshots being handed
        # out during the mixed-version window are always V6.
        assert keeper_utils.is_leader(cluster, node1)

        upgrade(node3, prefix)
        keeper_utils.wait_until_connected(cluster, node3)

        # Drive a still-old follower into a snapshot install from the old leader
        # while an upgraded follower is already in the cluster.
        node2.stop_clickhouse()
        write_batch_on(cluster, node1, "/while_node2_down", 200)
        assert create_snapshot(cluster, node1) == V6

        node2.start_clickhouse(start_wait_sec=120)
        keeper_utils.wait_until_connected(cluster, node2)

        assert wait_for_log(
            node2, "Saving snapshot", timeout=120
        ), "expected node2 to be caught up by a snapshot install, not by log replay"
        assert not node2.contains_in_log(UNSUPPORTED_VERSION_ERROR)

        # The old node accepted the leader's V6 snapshot and is serving.
        assert_batch_visible(cluster, [node2], "/while_node2_down", 200)

        # Finish the rolling upgrade: remaining follower, then the leader.
        for node in (node2, node1):
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
# RECOVERABLE: transient breakage that must clear once the node is upgraded.
# ---------------------------------------------------------------------------


def test_old_node_broken_by_v8_snapshot_recovers_when_upgraded():
    """An old node handed a `V8` snapshot must recover once it gets the new binary.

    This is the residual risk of the followers-first order: if the still-old
    leader is lost involuntarily part-way through the rollout, leadership moves to
    an already-upgraded node that writes `V8`, and the old node coming back is
    handed a snapshot it cannot read. Since every node is upgraded eventually,
    what matters is not that the disruption happens but that rolling the node
    forward clears it completely and loses nothing.

    This test fails if the node does not come back, if it does not rejoin the
    quorum, or if any znode written while it was down is missing afterwards.
    """
    prefix = "small_snap_keeper"
    cluster, nodes = make_cluster(prefix)
    node1, node2, node3 = nodes
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)
        assert keeper_utils.is_leader(cluster, node1)

        write_batch_on(cluster, node1, "/before", 100)
        assert_batch_visible(cluster, nodes, "/before", 100)

        # Followers first, each restart also raising the write version, which is
        # the rollout in use. The leader is left for last.
        for node in (node3, node2):
            upgrade(node, prefix, write_snapshot_version=V8)
            keeper_utils.wait_until_connected(cluster, node)

        # The still-old leader is lost involuntarily before its turn comes.
        node1.stop_clickhouse(kill=True)

        # Leadership lands on an upgraded node, which writes V8 from now on.
        force_leader(cluster, node3)
        assert create_snapshot(cluster, node3) == V8

        # Move far enough ahead that the log node1 needs has been compacted, so
        # rejoining requires a snapshot install rather than log replay. node2 and
        # node3 still form a quorum, so writes keep working.
        write_batch_on(cluster, node3, "/while_old_leader_down", 200)
        assert create_snapshot(cluster, node3) == V8

        # node1 comes back still on the old binary, before anyone upgrades it.
        # It may abort while starting, so the start is not required to succeed;
        # there is deliberately no assertion inside this block.
        try:
            node1.start_clickhouse(start_wait_sec=60, retry_start=False)
        except Exception:
            pass

        # It was offered a snapshot, which is the precondition for this scenario.
        # If this ever stops holding, the test is no longer exercising the path it
        # was written for and needs revisiting rather than silently passing.
        assert wait_for_log(
            node1, "Saving snapshot", timeout=120
        ), "node1 was never offered a snapshot, so the scenario did not reproduce"
        assert wait_for_log(
            node1, UNSUPPORTED_VERSION_ERROR, timeout=120
        ), f"expected {UNSUPPORTED_VERSION_ERROR!r} while node1 was still old"

        # The rollout continues and node1 finally gets the new binary. Everything
        # from here on must hold, or this scenario is not survivable.
        upgrade_stopped_node(node1, prefix, write_snapshot_version=V8)
        keeper_utils.wait_until_connected(cluster, node1)

        # Recovered: it reads the V8 snapshot, and nothing written while it was
        # down was lost.
        assert_batch_visible(cluster, [node1], "/before", 100)
        assert_batch_visible(cluster, [node1], "/while_old_leader_down", 200)

        # Recovered as a real quorum member, not just as a reader: a write issued
        # against it is accepted and replicated everywhere.
        write_batch_on(cluster, node1, "/after_recovery", 20)
        assert_batch_visible(cluster, nodes, "/after_recovery", 20)

        # And it participates in snapshots again, at the new version.
        assert create_snapshot(cluster, node1) == V8

        # Full cluster restart from V8 snapshots keeps every batch.
        for node in nodes:
            node.restart_clickhouse(stop_start_wait_sec=120, kill=True)
            keeper_utils.wait_until_connected(cluster, node)
        assert_batch_visible(cluster, nodes, "/before", 100)
        assert_batch_visible(cluster, nodes, "/while_old_leader_down", 200)
        assert_batch_visible(cluster, nodes, "/after_recovery", 20)
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# UNSAFE: raising the version at all, regardless of upgrade order.
# ---------------------------------------------------------------------------


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
