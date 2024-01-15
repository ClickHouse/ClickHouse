#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as keeper_utils
from minio.deleteobjects import DeleteObject

import os

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    with_minio=True,
    with_hdfs=True,
)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def stop_clickhouse(cluster, cleanup_disks):
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


def setup_storage(cluster, storage_config, cleanup_disks):
    stop_clickhouse(cluster, cleanup_disks)
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
    keeper_utils.wait_until_connected(cluster, node)


def setup_local_storage(cluster):
    setup_storage(
        cluster,
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


def get_local_files(path):
    files = node.exec_in_container(["ls", path]).strip().split("\n")
    files.sort()
    return files


def get_local_logs():
    return get_local_files("/var/lib/clickhouse/coordination/logs")


def get_local_snapshots():
    return get_local_files("/var/lib/clickhouse/coordination/snapshots")


def test_supported_disk_types(started_cluster):
    node.stop_clickhouse()
    node.start_clickhouse()
    node.contains_in_log("Disk type 'hdfs' is not supported for Keeper")


def test_logs_with_disks(started_cluster):
    setup_local_storage(started_cluster)

    node_zk = get_fake_zk("node")
    try:
        node_zk.create("/test")
        for _ in range(30):
            node_zk.create("/test/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        previous_log_files = get_local_logs()

        setup_storage(
            started_cluster,
            "<log_storage_disk>log_s3_plain<\\/log_storage_disk>"
            "<latest_log_storage_disk>log_local<\\/latest_log_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
            cleanup_disks=False,
        )

        # all but the latest log should be on S3
        s3_log_files = list_s3_objects(started_cluster, "logs/")
        assert set(s3_log_files) == set(previous_log_files[:-1])
        local_log_files = get_local_logs()
        assert len(local_log_files) == 1
        assert local_log_files[0] == previous_log_files[-1]

        previous_log_files = s3_log_files + local_log_files

        node_zk = get_fake_zk("node")

        for _ in range(30):
            node_zk.create("/test/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        log_files = list_s3_objects(started_cluster, "logs/")
        local_log_files = get_local_logs()
        assert len(local_log_files) == 1

        log_files.extend(local_log_files)
        assert set(log_files) != previous_log_files

        previous_log_files = log_files

        setup_storage(
            started_cluster,
            "<old_log_storage_disk>log_s3_plain<\\/old_log_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
            cleanup_disks=False,
        )

        local_log_files = get_local_logs()
        assert set(local_log_files) == set(previous_log_files)

        node_zk = get_fake_zk("node")

        for child in node_zk.get_children("/test"):
            assert node_zk.get(f"/test/{child}")[0] == b"somedata"

    finally:
        stop_zk(node_zk)


def test_snapshots_with_disks(started_cluster):
    setup_local_storage(started_cluster)

    node_zk = get_fake_zk("node")
    try:
        node_zk.create("/test2")
        for _ in range(30):
            node_zk.create("/test2/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node, "csnp")
        node.wait_for_log_line(
            f"Created persistent snapshot {snapshot_idx}", look_behind_lines=1000
        )

        previous_snapshot_files = get_local_snapshots()

        setup_storage(
            started_cluster,
            "<snapshot_storage_disk>snapshot_s3_plain<\\/snapshot_storage_disk>"
            "<latest_snapshot_storage_disk>snapshot_local<\\/latest_snapshot_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>",
            cleanup_disks=False,
        )

        ## all but the latest log should be on S3
        s3_snapshot_files = list_s3_objects(started_cluster, "snapshots/")
        assert set(s3_snapshot_files) == set(previous_snapshot_files[:-1])
        local_snapshot_files = get_local_snapshots()
        assert len(local_snapshot_files) == 1
        assert local_snapshot_files[0] == previous_snapshot_files[-1]

        previous_snapshot_files = s3_snapshot_files + local_snapshot_files

        node_zk = get_fake_zk("node")

        for _ in range(30):
            node_zk.create("/test2/somenode", b"somedata", sequence=True)

        stop_zk(node_zk)

        snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node, "csnp")
        node.wait_for_log_line(
            f"Created persistent snapshot {snapshot_idx}", look_behind_lines=1000
        )

        snapshot_files = list_s3_objects(started_cluster, "snapshots/")
        local_snapshot_files = get_local_snapshots()
        assert len(local_snapshot_files) == 1

        snapshot_files.extend(local_snapshot_files)

        previous_snapshot_files = snapshot_files

        setup_storage(
            started_cluster,
            "<old_snapshot_storage_disk>snapshot_s3_plain<\\/old_snapshot_storage_disk>"
            "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>"
            "<log_storage_disk>log_local<\\/log_storage_disk>",
            cleanup_disks=False,
        )

        local_snapshot_files = get_local_snapshots()
        assert set(local_snapshot_files) == set(previous_snapshot_files)

        node_zk = get_fake_zk("node")

        for child in node_zk.get_children("/test2"):
            assert node_zk.get(f"/test2/{child}")[0] == b"somedata"

    finally:
        stop_zk(node_zk)
