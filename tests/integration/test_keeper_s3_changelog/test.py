#!/usr/bin/env python3
import os

import pytest
from minio.deleteobjects import DeleteObject

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster, is_arm

if is_arm():
    pytestmark = pytest.mark.skip


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
cluster = ClickHouseCluster(__file__)
node_logs = cluster.add_instance(
    "node_logs",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
    with_minio=True,
    # with_remote_database_disk=True
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
    files = node.exec_in_container(["ls", path]).strip().split("\n")
    files.sort()
    return files


def get_local_logs(node):
    return get_local_files("/var/lib/clickhouse/coordination/logs", node)


def get_local_snapshots(node):
    return get_local_files("/var/lib/clickhouse/coordination/snapshots", node)


def test_correctness(started_cluster):
    setup_local_storage(started_cluster, node_logs)

    node_zk = get_fake_zk("node_logs")
    try:
        node_zk.create("/test")
        for _ in range(30):
            node_zk.create_async("/test/somenode", b"somedata", sequence=True)

        node_logs.wait_for_log_line(
            "RaftInstance: commit upto 33",
            look_behind_lines=1000,
        )

        for child in node_zk.get_children("/test"):
            assert node_zk.get(f"/test/{child}")[0] == b"somedata"

        # stop_zk(node_zk)

        # previous_log_files = get_local_logs(node_logs)

        # setup_storage(
        #     started_cluster,
        #     node_logs,
        #     "<log_storage_disk>log_s3_plain<\\/log_storage_disk>"
        #     "<latest_log_storage_disk>log_local<\\/latest_log_storage_disk>"
        #     "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
        #     cleanup_disks=False,
        # )

        # node_logs.wait_for_log_line(
        #     "KeeperLogStore: Continue to write into changelog_34_36.bin",
        #     look_behind_lines=2000,
        # )

        # # all but the latest log should be on S3
        # s3_log_files = list_s3_objects(started_cluster, "logs/")
        # assert set(s3_log_files) == set(previous_log_files[:-1])
        # local_log_files = get_local_logs(node_logs)
        # assert len(local_log_files) == 1
        # assert local_log_files[0] == previous_log_files[-1]

        # previous_log_files = s3_log_files + local_log_files

        # node_zk = get_fake_zk("node_logs")

        # for _ in range(30):
        #     node_zk.create("/test/somenode", b"somedata", sequence=True)

        # stop_zk(node_zk)

        # log_files = list_s3_objects(started_cluster, "logs/")
        # local_log_files = get_local_logs(node_logs)
        # assert len(local_log_files) == 1

        # log_files.extend(local_log_files)
        # assert set(log_files) != previous_log_files

        # previous_log_files = log_files

        # setup_storage(
        #     started_cluster,
        #     node_logs,
        #     "<old_log_storage_disk>log_s3_plain<\\/old_log_storage_disk>"
        #     "<log_storage_disk>log_local<\\/log_storage_disk>"
        #     "<snapshot_storage_disk>snapshot_local<\\/snapshot_storage_disk>",
        #     cleanup_disks=False,
        # )

        # local_log_files = get_local_logs(node_logs)
        # assert set(local_log_files) == set(previous_log_files)

        # node_zk = get_fake_zk("node_logs")

        # for child in node_zk.get_children("/test"):
        #     assert node_zk.get(f"/test/{child}")[0] == b"somedata"

    finally:
        stop_zk(node_zk)
