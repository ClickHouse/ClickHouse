import time
import pytest
import logging
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper_config_root_a.xml')

node1 = cluster.add_instance('node1', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_root_a.xml"])
node2 = cluster.add_instance('node2', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_root_a.xml"])
node3 = cluster.add_instance('node3', with_zookeeper=True,
                                main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_root_b.xml"])

def create_zk_roots(zk):
    zk.ensure_path('/root_a')
    zk.ensure_path('/root_b')
    logging.debug(f"Create ZK roots:{zk.get_children('/')}")

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_zk_roots)
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def test_chroot_with_same_root(started_cluster):
    for i, node in enumerate([node1, node2]):
        node.query('DROP TABLE IF EXISTS simple SYNC')
        node.query('''
        CREATE TABLE simple (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
        '''.format(replica=node.name))
        for j in range(2):  # Second insert to test deduplication
            node.query("INSERT INTO simple VALUES ({0}, {0})".format(i))

    time.sleep(1)

    assert node1.query('select count() from simple').strip() == '2'
    assert node2.query('select count() from simple').strip() == '2'

def test_chroot_with_different_root(started_cluster):
    for i, node in [(1, node1), (3, node3)]:
        node.query('DROP TABLE IF EXISTS simple_different SYNC')
        node.query('''
        CREATE TABLE simple_different (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple_different', '{replica}', date, id, 8192);
        '''.format(replica=node.name))
        for j in range(2):  # Second insert to test deduplication
            node.query("INSERT INTO simple_different VALUES ({0}, {0})".format(i))

    assert node1.query('select count() from simple_different').strip() == '1'
    assert node3.query('select count() from simple_different').strip() == '1'
