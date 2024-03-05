#!/usr/bin/env python3

import time
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager
from .utils import get_remote_paths


DISK_NAME = "reacquire"
GC_SLEEP_SEC = 5
GC_LOCK_PATH = f"vfs/{DISK_NAME}/gc_lock"


@pytest.fixture(scope="module")
def started_cluster(request):
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_gc_remove_obsolete(started_cluster):
    node1: ClickHouseInstance = started_cluster.instances["node1"]
    bucket = started_cluster.minio_bucket
    minio = started_cluster.minio_client
    table = "test_gc_delete"

    # Upload some files to the object storage
    node1.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node1.query(f"INSERT INTO {table} VALUES (0)")
    node1.query(f"OPTIMIZE TABLE {table} FINAL")

    # Get all object keys related to the table
    table_objects = get_remote_paths(node1, table)
    bucket_objects = {
        obj.object_name for obj in minio.list_objects(bucket, "data/", recursive=True)
    }

    # All objects related to the table are present in bucket
    assert all([obj in bucket_objects for obj in table_objects])

    # Unlink all files related to the table
    node1.query(f"DROP TABLE {table} SYNC")
    # Wait one GC iteration
    time.sleep(GC_SLEEP_SEC * 1.5)

    bucket_objects = {
        obj.object_name for obj in minio.list_objects(bucket, "data/", recursive=True)
    }
    # Check that all objects related to the table were deleted by GC
    assert not any([obj in bucket_objects for obj in table_objects])


def test_optimistic_lock(started_cluster):
    node1: ClickHouseInstance = started_cluster.instances["node1"]
    node2: ClickHouseInstance = started_cluster.instances["node2"]
    zk = started_cluster.get_kazoo_client("zoo1")
    table = "test_optimistic_lock"
    # Stop second node to prevent interfering
    node2.stop_clickhouse()

    # Inject delay before releasing optimistic lock on node1
    # that vfs log has been processed by node2
    node1.query("SYSTEM ENABLE FAILPOINT vfs_gc_optimistic_lock_delay")

    # Make some disk activity
    node1.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node1.query(f"INSERT INTO {table} VALUES (0)")

    node1.wait_for_log_line("GC acquired lock")

    # Update lock node version
    value, _ = zk.get(GC_LOCK_PATH)
    zk.set(GC_LOCK_PATH, value)

    node1.wait_for_log_line(
        "Skip GC transaction because optimistic lock node was already updated"
    )

    node1.query(f"DROP TABLE {table} SYNC")
    node1.query("SYSTEM DISABLE FAILPOINT vfs_gc_optimistic_lock_delay")
    node2.start_clickhouse()


# TODO myrrc check possible errors on merge and move
def test_session_breaks(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node1"]
    table = "table_session_breaks"

    # non-replicated MergeTree implies ZK data flow will be vfs-related
    node.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node.query(f"INSERT INTO {table} VALUES (0)")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.query_and_get_error(f"INSERT INTO {table} VALUES (1)")
        time.sleep(4)
    time.sleep(2)  # Wait for CH to reconnect to ZK before next GC run

    assert (
        int(node.count_in_log("VFSGC(reacquire): GC iteration finished")) > 1
    ), "GC must run at least twice"
    assert (
        int(node.count_in_log("Trying to establish a new connection with ZooKeeper"))
        > 1
    ), "ZooKeeper session must expire"

    node.query(f"INSERT INTO {table} VALUES (2)")
    assert int(node.query(f"SELECT count() FROM {table}")) == 2
    node.query(f"DROP TABLE {table} SYNC")


def test_ch_disks(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node1"]

    listing = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--loglevel=trace",
            "--save-logs",
            "--config-file=/etc/clickhouse-server/config.d/config.xml",
            "list-disks",
        ],
        privileged=True,
        user="root",
    )
    print(listing)
    assert listing == "default\nreacquire\n"

    listing = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--config-file=/etc/clickhouse-server/config.xml",
            "--loglevel=trace",
            "--save-logs",
            "--disk=reacquire",
            "list",
            "/",
        ],
        privileged=True,
        user="root",
    )

    print(listing)

    log = node.exec_in_container(
        [
            "/usr/bin/cat",
            "/var/log/clickhouse-server/clickhouse-disks.log",
        ],
        privileged=True,
        user="root",
    )
    print(log)

    assert "GC enabled: false" in log
    assert "GC started" not in log
