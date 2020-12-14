import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_cluster_node(started_cluster):
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 1") == "4\n"
    node1.query("CLUSTER PAUSE NODE node2:9000 ON CLUSTER test_cluster_node")
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "1\n"

    node1.query("CREATE database IF NOT EXISTS test ON CLUSTER test_cluster_node");

    node1.query("CLUSTER PAUSE NODE node3:9000 ON CLUSTER test_cluster_node")
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "2\n"
    node1.query("CLUSTER START NODE node3:9000 ON CLUSTER test_cluster_node")
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "1\n"
    node1.query("CLUSTER START NODE node2:9000 ON CLUSTER test_cluster_node")
    
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "0\n"
    assert node2.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "0\n"
    assert node3.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "0\n"
    assert node4.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_node' AND status = 2") == "0\n"