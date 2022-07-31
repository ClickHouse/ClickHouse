import pytest
from helpers.cluster import ClickHouseCluster
import os
from kazoo.client import KazooClient

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)

node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)

def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def test_snapshot_on_exit(started_cluster):
    zk_conn = get_fake_zk(node1);

    zk_conn.create("/some_path", b"some_data")

    node1.stop_clickhouse()
    assert node1.contains_in_log("Created persistent snapshot")

    node1.start_clickhouse()
    assert node1.contains_in_log("Loaded snapshot")

    node2.stop_clickhouse()
    assert not node2.contains_in_log("Created persistent snapshot")

    node2.start_clickhouse()
    assert node2.contains_in_log("No existing snapshots")
