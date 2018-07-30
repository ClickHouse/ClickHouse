import time
import pytest

from helpers.cluster import ClickHouseCluster


def _fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
        '''
            CREATE DATABASE test;

            CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}', date, id, 8192);
        '''.format(shard=shard, replica=node.name))

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml', 'configs/credentials1.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml', 'configs/credentials1.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def same_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node1, node2], 1)

        yield cluster

    finally:
        cluster.shutdown()

def test_same_credentials(same_credentials_cluster):
    node1.query("insert into test_table values ('2017-06-16', 111, 0)")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == '111\n'
    assert node2.query("SELECT id FROM test_table order by id") == '111\n'

    node2.query("insert into test_table values ('2017-06-17', 222, 1)")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == '111\n222\n'
    assert node2.query("SELECT id FROM test_table order by id") == '111\n222\n'


node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml', 'configs/no_credentials.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', main_configs=['configs/remote_servers.xml', 'configs/no_credentials.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def no_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node3, node4], 2)

        yield cluster

    finally:
        cluster.shutdown()


def test_no_credentials(no_credentials_cluster):
    node3.query("insert into test_table values ('2017-06-18', 111, 0)")
    time.sleep(1)

    assert node3.query("SELECT id FROM test_table order by id") == '111\n'
    assert node4.query("SELECT id FROM test_table order by id") == '111\n'

    node4.query("insert into test_table values ('2017-06-19', 222, 1)")
    time.sleep(1)

    assert node3.query("SELECT id FROM test_table order by id") == '111\n222\n'
    assert node4.query("SELECT id FROM test_table order by id") == '111\n222\n'

node5 = cluster.add_instance('node5', main_configs=['configs/remote_servers.xml', 'configs/credentials1.xml'], with_zookeeper=True)
node6 = cluster.add_instance('node6', main_configs=['configs/remote_servers.xml', 'configs/credentials2.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def different_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node5, node6], 3)

        yield cluster

    finally:
        cluster.shutdown()

def test_different_credentials(different_credentials_cluster):
    node5.query("insert into test_table values ('2017-06-20', 111, 0)")
    time.sleep(1)

    assert node5.query("SELECT id FROM test_table order by id") == '111\n'
    assert node6.query("SELECT id FROM test_table order by id") == ''

    node6.query("insert into test_table values ('2017-06-21', 222, 1)")
    time.sleep(1)

    assert node5.query("SELECT id FROM test_table order by id") == '111\n'
    assert node6.query("SELECT id FROM test_table order by id") == '222\n'

node7 = cluster.add_instance('node7', main_configs=['configs/remote_servers.xml', 'configs/credentials1.xml'], with_zookeeper=True)
node8 = cluster.add_instance('node8', main_configs=['configs/remote_servers.xml', 'configs/no_credentials.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def credentials_and_no_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node7, node8], 4)

        yield cluster

    finally:
        cluster.shutdown()

def test_credentials_and_no_credentials(credentials_and_no_credentials_cluster):
    node7.query("insert into test_table values ('2017-06-21', 111, 0)")
    time.sleep(1)

    assert node7.query("SELECT id FROM test_table order by id") == '111\n'
    assert node8.query("SELECT id FROM test_table order by id") == ''

    node8.query("insert into test_table values ('2017-06-22', 222, 1)")
    time.sleep(1)

    assert node7.query("SELECT id FROM test_table order by id") == '111\n'
    assert node8.query("SELECT id FROM test_table order by id") == '222\n'

