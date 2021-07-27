import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('127.0.0.1', main_configs=['configs/remote_servers.xml'], user_configs=['configs/settings.xml'], with_zookeeper=True)
node2 = cluster.add_instance('127.0.0.2', main_configs=['configs/remote_servers.xml'], user_configs=['configs/settings.xml'], with_zookeeper=False)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_cluster_node(started_cluster):
    node1.query("CREATE DATABASE test1 ON CLUSTER test_cluster_two_replicas")
    assert node1.query(" SELECT COUNT() FROM system.databases WHERE name = 'test1'").rstrip() == '1'
    assert node2.query(" SELECT COUNT() FROM system.databases WHERE name = 'test1'").rstrip() == '0'
