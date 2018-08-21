import pytest
import time
import sys

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

def _fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
        '''
            CREATE DATABASE test;

            CREATE TABLE real_table(date Date, id UInt32, dummy UInt32)
            ENGINE = MergeTree(date, id, 8192);

            CREATE TABLE other_table(date Date, id UInt32, dummy UInt32)
            ENGINE = MergeTree(date, id, 8192);

            CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}', date, id, 8192);
        '''.format(shard=shard, replica=node.name))


node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def normal_work():
    try:
        cluster.start()

        _fill_nodes([node1, node2], 1)

        yield cluster

    finally:
        cluster.shutdown()

def test_normal_work(normal_work):
    node1.query("insert into test_table values ('2017-06-16', 111, 0)")
    node1.query("insert into real_table values ('2017-06-16', 222, 0)")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == '111\n'
    assert node1.query("SELECT id FROM real_table order by id") == '222\n'
    assert node2.query("SELECT id FROM test_table order by id") == '111\n'

    node1.query("ALTER TABLE test_table REPLACE PARTITION 201706 FROM real_table")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == '222\n'
    assert node2.query("SELECT id FROM test_table order by id") == '222\n'

node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def drop_failover():
    try:
        cluster.start()

        _fill_nodes([node3, node4], 2)

        yield cluster

    finally:
        cluster.shutdown()

def test_drop_failover(drop_failover):
    node3.query("insert into test_table values ('2017-06-16', 111, 0)")
    node3.query("insert into real_table values ('2017-06-16', 222, 0)")
    time.sleep(1)

    assert node3.query("SELECT id FROM test_table order by id") == '111\n'
    assert node3.query("SELECT id FROM real_table order by id") == '222\n'
    assert node4.query("SELECT id FROM test_table order by id") == '111\n'


    with PartitionManager() as pm:
        # Hinder replication between replicas
        pm.partition_instances(node3, node4, port=9009)
        # Disconnect Node4 from zookeper
        pm.drop_instance_zk_connections(node4)

        node3.query("ALTER TABLE test_table REPLACE PARTITION 201706 FROM real_table")

        # Node3 replace is ok
        assert node3.query("SELECT id FROM test_table order by id") == '222\n'
        # Network interrupted -- replace is not ok, but it's ok
        assert node4.query("SELECT id FROM test_table order by id") == '111\n'

        #Drop partition on source node
        node3.query("ALTER TABLE test_table DROP PARTITION 201706")

    time.sleep(1)
    # connection restored
    counter = 0
    while counter < 10: # will lasts forever
        if 'Not found part' not in node4.query("select last_exception from system.replication_queue where type = 'REPLACE_RANGE'"):
            break
        time.sleep(1)
        counter += 1
    assert 'Not found part' not in node4.query("select last_exception from system.replication_queue where type = 'REPLACE_RANGE'")
    assert node4.query("SELECT id FROM test_table order by id") == ''

node5 = cluster.add_instance('node5', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node6 = cluster.add_instance('node6', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def replace_after_replace_failover():
    try:
        cluster.start()

        _fill_nodes([node5, node6], 3)

        yield cluster

    finally:
        cluster.shutdown()

def test_replace_after_replace_failover(replace_after_replace_failover):
    node5.query("insert into test_table values ('2017-06-16', 111, 0)")
    node5.query("insert into real_table values ('2017-06-16', 222, 0)")
    node5.query("insert into other_table values ('2017-06-16', 333, 0)")
    time.sleep(1)

    assert node5.query("SELECT id FROM test_table order by id") == '111\n'
    assert node5.query("SELECT id FROM real_table order by id") == '222\n'
    assert node5.query("SELECT id FROM other_table order by id") == '333\n'
    assert node6.query("SELECT id FROM test_table order by id") == '111\n'


    with PartitionManager() as pm:
        # Hinder replication between replicas
        pm.partition_instances(node5, node6, port=9009)
        # Disconnect Node6 from zookeper
        pm.drop_instance_zk_connections(node6)

        node5.query("ALTER TABLE test_table REPLACE PARTITION 201706 FROM real_table")

        # Node5 replace is ok
        assert node5.query("SELECT id FROM test_table order by id") == '222\n'
        # Network interrupted -- replace is not ok, but it's ok
        assert node6.query("SELECT id FROM test_table order by id") == '111\n'

        #Replace partition on source node
        node5.query("ALTER TABLE test_table REPLACE PARTITION 201706 FROM other_table")

        assert node5.query("SELECT id FROM test_table order by id") == '333\n'

    time.sleep(1)
    # connection restored
    counter = 0
    while counter < 10: # will lasts forever
        if 'Not found part' not in node6.query("select last_exception from system.replication_queue where type = 'REPLACE_RANGE'"):
            break
        time.sleep(1)
        counter += 1
    assert 'Not found part' not in node6.query("select last_exception from system.replication_queue where type = 'REPLACE_RANGE'")
    assert node6.query("SELECT id FROM test_table order by id") == '333\n'
