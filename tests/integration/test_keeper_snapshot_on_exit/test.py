import os

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

# Disable `with_remote_database_disk` as the test does not use the default Keeper.
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
)

node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
)


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_snapshot_on_exit(started_cluster):
    zk_conn = None
    try:
        zk_conn = get_fake_zk(node1)
        zk_conn.create("/some_path", b"some_data")

        node1.stop_clickhouse()
        assert node1.contains_in_log("Created persistent snapshot")

        node1.start_clickhouse()
        assert node1.contains_in_log("Loaded snapshot")

        node2.stop_clickhouse()
        assert not node2.contains_in_log("Created persistent snapshot")

        node2.start_clickhouse()
        assert node2.contains_in_log("No existing snapshots")
    finally:
        if zk_conn:
            if zk_conn.exists("/some_path"):
                zk_conn.delete("/some_path")
            zk_conn.stop()
            zk_conn.close()
