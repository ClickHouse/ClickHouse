#!/usr/bin/env python3

import time
import pytest
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


def test_already_processed_log_batch(started_cluster):
    """
    Test if GC:
    - processed a log batch
    - wrote new snapshot
    - removed old snapshot, and
    - failed to remove old log batch from ZK,
    next GC run will discard already processed batch
    """
    pass


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

    assert int(node.count_in_log("Removed lock for")) == 2, "GC must run twice"
    assert (
        int(node.count_in_log("Trying to establish a new connection with ZooKeeper"))
        > 1
    ), "ZooKeeper session must expire"

    node.query("INSERT INTO test VALUES (2)")
    assert int(node.query("SELECT count() FROM test")) == 2
