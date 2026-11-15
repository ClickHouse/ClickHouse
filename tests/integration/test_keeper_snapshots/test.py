#!/usr/bin/env python3

import os
import random
import string
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.test_tools import get_retry_number
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)

# Same configuration, but with compress_snapshots_with_zstd_format turned off, so that
# snapshots are written in ClickHouse's own compressed block format instead of zstd,
# and with the nodes digest enabled.
node_uncompressed = cluster.add_instance(
    "node_uncompressed",
    main_configs=["configs/enable_keeper.xml", "configs/uncompressed_snapshots.xml"],
    stay_alive=True,
)


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown(ignore_fatal=True)


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)
    return _fake_zk_instance


def restart_clickhouse():
    node.restart_clickhouse(kill=True)
    keeper_utils.wait_until_connected(cluster, node)


def test_state_after_restart(started_cluster, request):
    keeper_utils.wait_until_connected(started_cluster, node)
    node_zk = None
    node_zk2 = None
    try:
        node_zk = get_connection_zk("node")

        chroot = f"/test_state_after_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i])

        existing_children = []
        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/node" + str(i))
            else:
                existing_children.append("node" + str(i))

        restart_clickhouse()

        node_zk2 = get_connection_zk("node")
        node_zk2.chroot = chroot

        assert node_zk2.get("/")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/node" + str(i)) is None
            else:
                data, stat = node_zk2.get("/node" + str(i))
                assert len(data) == 123
                assert data == strs[i]
                assert stat.ephemeralOwner == 0

        assert list(sorted(existing_children)) == list(
            sorted(node_zk2.get_children("/"))
        )
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass


def test_ephemeral_after_restart(started_cluster, request):
    keeper_utils.wait_until_connected(started_cluster, node)
    node_zk = None
    node_zk2 = None
    try:
        node_zk = get_connection_zk("node")

        session_id = node_zk._session_id

        chroot = f"/test_ephemeral_after_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i], ephemeral=True)

        existing_children = []
        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/node" + str(i))
            else:
                existing_children.append("node" + str(i))

        restart_clickhouse()

        node_zk2 = get_connection_zk("node")
        node_zk2.chroot = chroot

        assert node_zk2.get("/")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/node" + str(i)) is None
            else:
                data, stat = node_zk2.get("/node" + str(i))
                assert len(data) == 123
                assert data == strs[i]
                assert stat.ephemeralOwner == session_id
        assert list(sorted(existing_children)) == list(
            sorted(node_zk2.get_children("/"))
        )
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass


def test_invalid_snapshot(started_cluster, request):
    keeper_utils.wait_until_connected(started_cluster, node)
    node_zk = None
    try:
        node_zk = get_connection_zk("node")

        chroot = f"/test_invalid_snapshot_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        keeper_utils.send_4lw_cmd(started_cluster, node, "csnp")
        node.stop_clickhouse()
        snapshots = (
            node.exec_in_container(["ls", "/var/lib/clickhouse/coordination/snapshots"])
            .strip()
            .split("\n")
        )

        def snapshot_sort_key(snapshot_name):
            # snapshot_<idx>[_<uuid>].bin[.zstd] — the index is the second '_'-separated token
            return int(snapshot_name.split("_")[1].split(".")[0])

        snapshots.sort(key=snapshot_sort_key)
        last_snapshot = snapshots[-1]
        node.exec_in_container(
            [
                "truncate",
                "-s",
                "0",
                f"/var/lib/clickhouse/coordination/snapshots/{last_snapshot}",
            ]
        )
        node.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
        assert node.contains_in_log("Failure to load from latest snapshot with index")
        assert node.contains_in_log(
            "Manual intervention is necessary for recovery. Problematic snapshot can be removed but it will lead to data loss"
        )

        node.stop_clickhouse()
        node.exec_in_container(
            [
                "rm",
                f"/var/lib/clickhouse/coordination/snapshots/{last_snapshot}",
            ]
        )
        node.start_clickhouse()
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()
        except:
            pass


def test_snapshot_size(started_cluster, request):
    keeper_utils.wait_until_connected(started_cluster, node)
    node_zk = None
    try:
        node_zk = get_connection_zk("node")

        chroot = f"/test_state_size_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i])

        node_zk.stop()
        node_zk.close()

        keeper_utils.send_4lw_cmd(started_cluster, node, "csnp")
        node.wait_for_log_line("Created persistent snapshot")

        def get_snapshot_size():
            return int(
                next(
                    filter(
                        lambda line: "zk_latest_snapshot_size" in line,
                        keeper_utils.send_4lw_cmd(started_cluster, node, "mntr").split(
                            "\n"
                        ),
                    )
                ).split("\t")[1]
            )

        assert get_snapshot_size() != 0
        restart_clickhouse()
        assert get_snapshot_size() != 0
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

        except:
            pass


@pytest.mark.parametrize("node_name", ["node", "node_uncompressed"])
def test_snapshot_survives_restart(started_cluster, request, node_name):
    keeper_node = started_cluster.instances[node_name]
    keeper_utils.wait_until_connected(started_cluster, keeper_node)
    node_zk = None
    try:
        node_zk = get_connection_zk(keeper_node.name)

        chroot = f"/test_snapshot_survives_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot
        for i in range(100):
            node_zk.create("/node" + str(i), random_string(123).encode())

        # 'csnp' refuses while an automatic snapshot is still in flight, and also whenever the
        # committed index is already covered by the latest snapshot. Advance the log with a write
        # before each retry, otherwise the second refusal repeats forever.
        for _ in range(20):
            snapshot_idx = keeper_utils.send_4lw_cmd(
                started_cluster, keeper_node, "csnp"
            ).strip()
            if snapshot_idx.isdigit():
                break
            node_zk.set("/node0", random_string(123).encode())
            time.sleep(1)
        else:
            assert False, f"csnp kept refusing to schedule: {snapshot_idx!r}"

        node_zk.stop()
        node_zk.close()
        node_zk = None

        # 'csnp' only schedules the snapshot, so wait for the index it returned: the log already
        # holds lines from the snapshots taken every snapshot_distance records.
        keeper_node.wait_for_log_line(
            f"Created persistent snapshot {snapshot_idx} with path"
        )

        snapshot_sizes = keeper_node.exec_in_container(
            [
                "bash",
                "-c",
                "stat -c %s "
                f"/var/lib/clickhouse/coordination/snapshots/snapshot_{snapshot_idx}_* || true",
            ]
        ).split()
        assert snapshot_sizes, "no snapshot file was written"
        assert all(int(size) > 0 for size in snapshot_sizes), (
            f"snapshot written as an empty file: sizes {snapshot_sizes}"
        )

        keeper_node.restart_clickhouse(kill=True)
        keeper_utils.wait_until_connected(started_cluster, keeper_node)

        node_zk = get_connection_zk(keeper_node.name)
        node_zk.chroot = chroot
        assert len(node_zk.get_children("/")) == 100
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()
        except:
            pass


def run_snapshot_analyzer(keeper_node, snapshot_path, extra_args=""):
    """Returns (exit_code, combined stdout+stderr) of `clickhouse keeper-utils snapshot-analyzer`."""
    output = keeper_node.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse keeper-utils snapshot-analyzer "
            f"--snapshot-path {snapshot_path} {extra_args} 2>&1; echo EXIT_CODE=$?",
        ]
    )
    lines = output.splitlines()
    assert lines[-1].startswith("EXIT_CODE="), output
    return int(lines[-1][len("EXIT_CODE=") :]), "\n".join(lines[:-1])


def parse_stat(output, name):
    """Extracts the integer after `  <name>: ` from the analyzer output."""
    for line in output.splitlines():
        if line.strip().startswith(name + ":"):
            return int(line.split(":", 1)[1].strip().split()[0])
    assert False, f"no '{name}' line in output:\n{output}"


# 'node' writes zstd-compressed snapshots without digest, 'node_uncompressed' writes
# CompressedWriteBuffer-compressed snapshots with digest.
@pytest.mark.parametrize("node_name", ["node", "node_uncompressed"])
def test_snapshot_analyzer(started_cluster, request, node_name):
    keeper_node = started_cluster.instances[node_name]
    keeper_utils.wait_until_connected(started_cluster, keeper_node)
    snapshots_dir = "/var/lib/clickhouse/coordination/snapshots"
    node_zk = None
    try:
        node_zk = get_connection_zk(keeper_node.name)

        chroot = f"/test_snapshot_analyzer_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot
        for i in range(100):
            node_zk.create("/node" + str(i), random_string(123).encode())
        node_zk.create("/ephemeral", b"e", ephemeral=True)
        node_zk.create("/big", b"x" * 1000)

        # Same dance as in test_snapshot_survives_restart: 'csnp' refuses while an automatic
        # snapshot is in flight or when the committed index is already covered by a snapshot.
        for _ in range(20):
            snapshot_idx = keeper_utils.send_4lw_cmd(
                started_cluster, keeper_node, "csnp"
            ).strip()
            if snapshot_idx.isdigit():
                break
            node_zk.set("/node0", random_string(123).encode())
            time.sleep(1)
        else:
            assert False, f"csnp kept refusing to schedule: {snapshot_idx!r}"

        keeper_node.wait_for_log_line(
            f"Created persistent snapshot {snapshot_idx} with path"
        )
        snapshot_files = keeper_node.exec_in_container(
            ["bash", "-c", f"ls {snapshots_dir} | grep ^snapshot_{snapshot_idx}_"]
        ).split()
        assert len(snapshot_files) == 1, snapshot_files
        snapshot_file = snapshot_files[0]
        if node_name == "node":
            assert snapshot_file.endswith(".bin.zstd"), snapshot_file
        else:
            assert snapshot_file.endswith(".bin"), snapshot_file

        # A single snapshot file, with every optional section enabled.
        exit_code, output = run_snapshot_analyzer(
            keeper_node,
            f"{snapshots_dir}/{snapshot_file}",
            "--with-node-stats --subtrees-limit 3 --sample-size 5 --top-children 3",
        )
        assert exit_code == 0, output
        assert f"=== Snapshot: {snapshots_dir}/{snapshot_file} ===" in output
        assert parse_stat(output, "Last committed log index") == int(snapshot_idx)
        # The chroot itself, 102 children, the root and the system nodes; the keeper is shared
        # with the other tests of this module, so only lower bounds can be checked.
        assert parse_stat(output, "Number of nodes") >= 104
        assert parse_stat(output, "Ephemeral nodes") >= 1
        assert parse_stat(output, "Nodes with non-empty data") >= 103
        assert parse_stat(output, "Max node data size") >= 1000
        assert parse_stat(output, "Total node data size") >= 100 * 123 + 1000
        assert parse_stat(output, "Max number of children") >= 102
        assert parse_stat(output, "Number of sessions") >= 1
        assert "  Top 3 nodes by children count:\n" in output
        assert f"    {chroot}: 102 children\n" in output
        assert "  Random sample of up to 5 nodes (path: data size):\n" in output
        assert "  Predicted KeeperMemNodesStorage memory usage" in output
        assert "  Top 3 biggest subtrees:\n" in output
        assert f"    {chroot}: 102 descendants\n" in output
        if node_name == "node":
            assert "  Digest: none" in output
        else:
            assert "  Digest: none" not in output
            assert parse_stat(output, "Digest") != 0

        # Directory mode: every snapshot is analyzed, newest first.
        exit_code, output = run_snapshot_analyzer(keeper_node, snapshots_dir)
        assert exit_code == 0, output
        analyzed = [
            line[len("=== Snapshot: ") : -len(" ===")]
            for line in output.splitlines()
            if line.startswith("=== Snapshot: ")
        ]
        assert snapshot_file in analyzed, output
        assert f"Found {len(analyzed)} snapshots in {snapshots_dir}:" in output
        indices = [int(name.split("_")[1]) for name in analyzed]
        assert indices == sorted(indices, reverse=True), analyzed
        assert output.count("  Number of nodes: ") == len(analyzed)

        # A broken snapshot makes the command fail, but in directory mode the other snapshots
        # are still analyzed.
        broken_dir = f"/tmp/test_snapshot_analyzer_{get_retry_number(request)}"
        keeper_node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -rf {broken_dir} && mkdir {broken_dir} && cp {snapshots_dir}/{snapshot_file} {broken_dir}/ "
                f"&& printf 'this is not a snapshot' > {broken_dir}/snapshot_1_00000000deadbeef.bin",
            ]
        )
        exit_code, output = run_snapshot_analyzer(
            keeper_node, f"{broken_dir}/snapshot_1_00000000deadbeef.bin"
        )
        assert exit_code != 0, output
        assert "Number of nodes" not in output

        exit_code, output = run_snapshot_analyzer(keeper_node, broken_dir)
        assert exit_code != 0, output
        assert f"Found 2 snapshots in {broken_dir}:" in output
        assert output.index(f"=== Snapshot: {snapshot_file} ===") < output.index(
            "=== Snapshot: snapshot_1_00000000deadbeef.bin ==="
        )
        assert output.count("  Number of nodes: ") == 1
        assert "Error analyzing snapshot snapshot_1_00000000deadbeef.bin" in output
        assert f"Failed to analyze 1 of 2 snapshots in {broken_dir}" in output
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()
        except:
            pass
