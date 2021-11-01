import time
import pytest
import logging
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_in_order.xml')

node1 = cluster.add_instance('node1', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_in_order.xml"])
node2 = cluster.add_instance('node2', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_in_order.xml"])
node3 = cluster.add_instance('node3', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_in_order.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def test_in_order(started_cluster):

    assert '1' == str(node1.exec_in_container(['bash', '-c', "lsof -a -i4 -i6 -itcp -w | grep 'roottestzookeeperconfigloadbalancing_zoo1_1.roottestzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l"], privileged=True, user='root')).strip()
    assert '1' == str(node2.exec_in_container(['bash', '-c', "lsof -a -i4 -i6 -itcp -w | grep 'roottestzookeeperconfigloadbalancing_zoo1_1.roottestzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l"], privileged=True, user='root')).strip()
    assert '1' == str(node3.exec_in_container(['bash', '-c', "lsof -a -i4 -i6 -itcp -w | grep 'roottestzookeeperconfigloadbalancing_zoo1_1.roottestzookeeperconfigloadbalancing_default:2181' | grep ESTABLISHED | wc -l"], privileged=True, user='root')).strip()
