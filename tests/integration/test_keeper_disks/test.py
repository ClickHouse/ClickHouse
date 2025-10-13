#!/usr/bin/env python3
import os

import pytest
from minio.deleteobjects import DeleteObject

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster, is_arm

import time
import logging
import re

if is_arm():
    pytestmark = pytest.mark.skip


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)
node_logs = cluster.add_instance(
    "node_logs",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    with_minio=True,
)

node_snapshot = cluster.add_instance(
    "node_snapshot",
    main_configs=["configs/enable_keeper_snapshot.xml"],
    stay_alive=True,
    with_minio=True,
)


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
    except:
        pass


def stop_clickhouse(cluster, node, cleanup_disks):
    node.stop_clickhouse()

    if not cleanup_disks:
        return

    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/logs"])
    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"])

    s3_objects = list_s3_objects(cluster, prefix="")
    if len(s3_objects) == 0:
        return

    assert (
        len(
            list(
                cluster.minio_client.remove_objects(
                    cluster.minio_bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )


def setup_storage(cluster, node, storage_config, cleanup_disks):
    stop_clickhouse(cluster, node, cleanup_disks)
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/enable_keeper.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
        "<!-- DISK DEFINITION PLACEHOLDER -->",
        storage_config,
    )
    node.start_clickhouse()
    # complete readiness checks that the sessions can be established,
    # but it creates sesssion for this, which will create one more record in log,
    # but this test is very strict on number of entries in the log,
    # so let's avoid this extra check and rely on retry policy
    keeper_utils.wait_until_connected(cluster, node, wait_complete_readiness=False)


def setup_local_storage(cluster, node):
    setup_storage(
        cluster,
        node,
        "<log_storage_disk>log_local<\\/log_storage_disk>"
        "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
        cleanup_disks=True,
    )


def list_s3_objects(cluster, prefix=""):
    minio = cluster.minio_client
    prefix_len = len(prefix)
    return [
        obj.object_name[prefix_len:]
        for obj in minio.list_objects(
            cluster.minio_bucket, prefix=prefix, recursive=True
        )
    ]


def get_local_files(path, node):
    # Be robust to empty directories and multi-column ls defaults.
    out = node.exec_in_container(["ls", "-1", path]).strip()
    files = [] if not out else out.split("\n")
    files.sort()
    return files


LOG_FILE_RE = re.compile(r"^changelog_(\d+)_(\d+)\.bin$")


def _parse_log_range(name):
    m = LOG_FILE_RE.match(name)
    assert m, f"Unexpected log filename: {name}"
    a, b = int(m.group(1)), int(m.group(2))
    assert a <= b, f"Bad range in {name}"
    return a, b


def _last_log_filename(names):
    # Choose the file with the highest end zxid
    return max(names, key=lambda n: _parse_log_range(n)[1])


def get_local_logs(node):
    files = get_local_files("/var/lib/clickhouse/coordination/logs", node)
    files = [f for f in files if LOG_FILE_RE.match(f)]
    # Sort by end zxid to ensure "last" is truly the most recent by range end.
    files.sort(key=lambda n: _parse_log_range(n)[1])
    return files


def get_local_snapshots(node):
    return get_local_files("/var/lib/clickhouse/coordination/snapshots", node)


def list_s3_log_bins(cluster):
    # Only the changelog .bin files under logs/
    return [name for name in list_s3_objects(cluster, "logs/") if LOG_FILE_RE.match(name)]


def test_logs_with_disks(started_cluster):
    setup_local_storage(started_cluster, node_logs)

    node_zk = get_fake_zk("node_logs")
    try:
        node_zk.create("/test")
        for _ in range(30):
            node_zk.create("/test/somenode", b"somedata", sequence=True)

        # Compaction removes some old changelogs; exact numbers vary between runs.
        node_logs.wait_for_log_line(
            r"Removed changelog changelog_[0-9]+_[0-9]+\.bin because of compaction"
        )

        stop_zk(node_zk)

        previous_log_files = get_local_logs(node_logs)

        setup_storage(
            started_cluster,
            node_logs,
            "<log_storage_disk>log_s3_plain<\\/log_storage_disk>"
            "<latest_log_storage_disk>log_local<\\/latest_log_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
            cleanup_disks=False,
        )

        # After restart we should continue writing into the same last local file.
        last_local_before = _last_log_filename(previous_log_files)
        node_logs.wait_for_log_line(
            rf"KeeperLogStore: Continue to write into {re.escape(last_local_before)}"
        )

        def get_single_local_log_file():
            local_log_files = get_local_logs(node_logs)
            start_time = time.time()
            while len(local_log_files) != 1:
                logging.debug(f"Local log files: {local_log_files}")
                assert (
                    time.time() - start_time < 60
                ), "local_log_files size is not equal to 1 after 60s"
                time.sleep(1)
                local_log_files = get_local_logs(node_logs)
            return local_log_files

        # all but the latest log should be on S3
        local_log_files = get_single_local_log_file()
        assert local_log_files[0] == previous_log_files[-1]
        s3_log_files = list_s3_log_bins(started_cluster)
        assert set(s3_log_files) == set(previous_log_files[:-1])

        previous_log_files = s3_log_files + local_log_files

        node_zk = get_fake_zk("node_logs")

        for _ in range(30):
            node_zk.create("/test/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        local_log_files = get_single_local_log_file()
        log_files = list_s3_log_bins(started_cluster)

        log_files.extend(local_log_files)
        assert set(log_files) != set(previous_log_files)

        previous_log_files = log_files

        setup_storage(
            started_cluster,
            node_logs,
            "<old_log_storage_disk>log_s3_plain<\\/old_log_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
            cleanup_disks=False,
        )

        local_log_files = get_local_logs(node_logs)

        # Accept either exact equality OR a pure-merge; anything else fails.
        if set(local_log_files) != set(previous_log_files):
            prev = [_parse_log_range(x) for x in previous_log_files]
            now = [_parse_log_range(x) for x in local_log_files]

            # 1) No data loss: every previous range must be covered by exactly one current range.
            coverage = []
            for (a, b) in prev:
                covers = [i for i, (c, d) in enumerate(now) if c <= a and d >= b]
                coverage.append(covers)

            missing = [prev[i] for i, covers in enumerate(coverage) if not covers]
            assert not missing, (
                "Missing coverage for previous log ranges: "
                f"{missing}; now={now}; prev={prev}"
            )

            # 2) Forbid splits: each previous range must map to exactly ONE current range.
            split = [prev[i] for i, covers in enumerate(coverage) if len(covers) > 1]
            assert not split, (
                "Unexpected split of previous ranges into multiple files: "
                f"{split}; now={now}; prev={prev}"
            )

            # 3) Only pure merges are allowed: number of files must not increase (no new writes here).
            assert len(now) <= len(prev), (
                "Unexpected increase in number of changelog files without new writes "
                f"(expected pure-merge or equality). prev={len(prev)}, now={len(now)}; "
                f"now={now}; prev={prev}"
            )

            # 4) Each current file must equal a contiguous union of its previous parts with exact bounds.
            now_parts = [[] for _ in range(len(now))]
            for i_prev, covers in enumerate(coverage):
                idx = covers[0]  # len==1 asserted above
                now_parts[idx].append(prev[i_prev])

            for (c, d), parts in zip(now, now_parts):
                assert parts, (
                    f"Current range {(c, d)} does not correspond to any previous ranges; "
                    f"unexpected shape change. now={now}; prev={prev}"
                )
                parts.sort(key=lambda x: x[0])
                # Must start at c
                assert parts[0][0] == c, (
                    "Merged range does not start at expected boundary: "
                    f"now={(c, d)}, parts={parts}"
                )
                # No gaps; allow adjacency or overlap
                cur_end = parts[0][1]
                for a, b in parts[1:]:
                    assert a <= cur_end + 1, (
                        "Gap inside merged range: "
                        f"prev_part={(a, b)}, accumulated_end={cur_end}, now={(c, d)}, parts={parts}"
                    )
                    cur_end = max(cur_end, b)
                # Must end at d
                assert cur_end == d, (
                    "Merged range does not end at expected boundary: "
                    f"now={(c, d)}, parts={parts}, accumulated_end={cur_end}"
                )

        node_zk = get_fake_zk("node_logs")

        for child in node_zk.get_children("/test"):
            assert node_zk.get(f"/test/{child}")[0] == b"somedata"

    finally:
        stop_zk(node_zk)


def test_snapshots_with_disks(started_cluster):
    setup_local_storage(started_cluster, node_snapshot)

    node_zk = get_fake_zk("node_snapshot")
    try:
        node_zk.create("/test2")
        for _ in range(30):
            node_zk.create("/test2/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node_snapshot, "csnp")
        node_snapshot.wait_for_log_line(f"Created persistent snapshot {snapshot_idx}")

        previous_snapshot_files = get_local_snapshots(node_snapshot)

        setup_storage(
            started_cluster,
            node_snapshot,
            "<snapshot_storage_disk>snapshot_s3_plain<\\/snapshot_storage_disk>"
            "<latest_snapshot_storage_disk>snapshot_local<\\/latest_snapshot_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>",
            cleanup_disks=False,
        )

        ## all but the latest log should be on S3
        s3_snapshot_files = list_s3_objects(started_cluster, "snapshots/")
        assert set(s3_snapshot_files) == set(previous_snapshot_files[:-1])
        local_snapshot_files = get_local_snapshots(node_snapshot)
        assert len(local_snapshot_files) == 1
        assert local_snapshot_files[0] == previous_snapshot_files[-1]

        previous_snapshot_files = s3_snapshot_files + local_snapshot_files

        node_zk = get_fake_zk("node_snapshot")

        for _ in range(30):
            node_zk.create("/test2/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node_snapshot, "csnp")
        node_snapshot.wait_for_log_line(f"Created persistent snapshot {snapshot_idx}")

        snapshot_files = list_s3_objects(started_cluster, "snapshots/")
        local_snapshot_files = get_local_snapshots(node_snapshot)
        assert len(local_snapshot_files) == 1

        snapshot_files.extend(local_snapshot_files)

        previous_snapshot_files = snapshot_files

        setup_storage(
            started_cluster,
            node_snapshot,
            "<old_snapshot_storage_disk>snapshot_s3_plain<\\/old_snapshot_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>",
            cleanup_disks=False,
        )

        local_snapshot_files = get_local_snapshots(node_snapshot)
        assert set(local_snapshot_files) == set(previous_snapshot_files)

        node_zk = get_fake_zk("node_snapshot")

        for child in node_zk.get_children("/test2"):
            assert node_zk.get(f"/test2/{child}")[0] == b"somedata"

    finally:
        stop_zk(node_zk)
