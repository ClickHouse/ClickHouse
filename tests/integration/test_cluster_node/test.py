import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('127.0.0.1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('127.0.0.2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_cluster_node(started_cluster):
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_two_shards' AND status = 1") == "2\n"
    node1.query("CLUSTER PAUSE NODE 127.0.0.2:9000 ON CLUSTER test_cluster_two_shards")
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_two_shards' AND status = 2") == "1\n"
 
    node1.query("CREATE database IF NOT EXISTS test ON CLUSTER test_cluster_two_shards")
    node1.query("CLUSTER START NODE 127.0.0.2:9000 ON CLUSTER test_cluster_two_shards")
    assert node1.query("SELECT COUNT() FROM system.clusters WHERE cluster = 'test_cluster_two_shardse' AND status = 2") == "0\n"