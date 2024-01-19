#!/usr/bin/env python3

import time
import io
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager


@pytest.fixture(scope="module")
def started_cluster(request):
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


# TODO myrrc check possible errors on merge and move
def test_reacquire_session(started_cluster):
    """
    Test VFS can work when ZK session breaks
    """
    node: ClickHouseInstance = started_cluster.instances["node"]
    # non-replicated MergeTree implies ZK data flow will be vfs-related
    node.query("CREATE TABLE test (i UInt32) ENGINE=MergeTree ORDER BY i")
    node.query("INSERT INTO test VALUES (0)")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.query_and_get_error("INSERT INTO test VALUES (1)")
        time.sleep(4)
    time.sleep(2)  # Wait for CH to reconnect to ZK before next GC run

    assert (
        int(node.count_in_log("VFSGC(reacquire): Removed lock for")) > 1
    ), "GC must run at least twice"
    assert (
        int(node.count_in_log("Trying to establish a new connection with ZooKeeper"))
        > 1
    ), "ZooKeeper session must expire"

    node.query("INSERT INTO test VALUES (2)")
    assert int(node.query("SELECT count() FROM test")) == 2


def test_already_processed_batch(started_cluster):
    """
    Test if GC:
    - processed a log batch
    - wrote new snapshot
    - removed old snapshot, and
    - failed to remove old log batch from ZK,
    next GC run will discard already processed batch
    """
    node: ClickHouseInstance = started_cluster.instances["node"]
    while int(node.count_in_log("VFSGC(already): Removed lock for")) < 1:
        time.sleep(0.3)

    zk: KazooClient = started_cluster.get_kazoo_client("zoo1")
    assert zk.get_children("/vfs_log/already/ops") == []
    for i in range(
        11, 15
    ):  # simulate already processed log (snapshot for 10 doesn't exist)
        zk.create(f"/vfs_log/already/ops/log-00000000{i}", b"invalid")

    started_cluster.minio_client.put_object(
        started_cluster.minio_bucket, "data/vfs/_already_14", io.StringIO(""), 0
    )

    time.sleep(5)
    assert int(node.count_in_log("found snapshot for 14, discarding this batch")) == 1
    assert zk.get_children("/vfs_log/already/ops") == []
    zk.stop()
