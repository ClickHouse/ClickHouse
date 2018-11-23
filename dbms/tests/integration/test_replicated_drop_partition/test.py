import pytest
import time
import sys

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

def _fill_nodes(nodes, users, passwords, cluster):
    for i in range(0,len(nodes)):
        node[i].query(
        '''
            CREATE DATABASE IF NOT EXISTS test;

            CREATE TABLE test_table{cluster} (date Date, id UInt32, dummy UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{cluster}/replicated', '{replica}', toYYYYMMDD(date), id, 8192);
        '''.format(cluster=cluster, replica=node.name), settings={"password": passwords[i]}, user=users[i])


node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], user_configs=['configs/node1.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], user_configs=['configs/node2.xml'], with_zookeeper=True)
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], user_configs=['configs/node3.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def normal_work():
    try:
	cluster.start()
	
	_fill_nodes([node1, node2],[default,default],[pTe5Tb0s,dtnDvTr9],1)
	_fill_nodes([node2, node3],[zhang,default],[azAUGBFl,ROgXGTDq],2)

        yield cluster

    finally:
        cluster.shutdown()

def test_normal_work(normal_work):
    node1.query("insert into test_table1 values ('2017-06-16', 111, 0)",settings={"password": pTe5Tb0s}, user=default)
    node1.query("insert into test_table1 values ('2017-06-16', 222, 0)",settings={"password": pTe5Tb0s}, user=default)
    node1.query("insert into test_table1 values ('2017-06-17', 333, 0)",settings={"password": pTe5Tb0s}, user=default)
    node1.query("insert into test_table1 values ('2017-06-17', 444, 0)",settings={"password": pTe5Tb0s}, user=default)
    node1.query("insert into test_table1 values ('2017-06-18', 555, 0)",settings={"password": pTe5Tb0s}, user=default)
    node1.query("insert into test_table1 values ('2017-06-18', 666, 0)",settings={"password": pTe5Tb0s}, user=default)


    assert_eq_with_retry(node1, "SELECT id FROM test_table1 order by id limit 1", '111',settings={"password": pTe5Tb0s}, user=default)
    assert_eq_with_retry(node2, "SELECT id FROM test_table1 order by id limit 1", '111',settings={"password": dtnDvTr9}, user=default)

    
    node2.query("insert into test_table2 values ('2017-06-17', 333, 0)",settings={"password": azAUGBFl}, user=zhang)
    node2.query("insert into test_table2 values ('2017-06-17', 444, 0)",settings={"password": azAUGBFl}, user=zhang)
    node2.query("insert into test_table2 values ('2017-06-18', 555, 0)",settings={"password": azAUGBFl}, user=zhang)
    node2.query("insert into test_table2 values ('2017-06-18', 666, 0)",settings={"password": azAUGBFl}, user=zhang)
    node2.query("insert into test_table2 values ('2017-06-19', 777, 0)",settings={"password": azAUGBFl}, user=zhang)
    node2.query("insert into test_table2 values ('2017-06-19', 888, 0)",settings={"password": azAUGBFl}, user=zhang)
    

    assert_eq_with_retry(node2, "SELECT id FROM test_table2 order by id limit 1", '333',settings={"password": azAUGBFl}, user=zhang)
    assert_eq_with_retry(node3, "SELECT id FROM test_table2 order by id limit 1", '333',settings={"password": ROgXGTDq}, user=default)

    node1.query("ALTER TABLE test_table1 DROP PARTITION 20170616")
    assert_eq_with_retry(node1, "SELECT id FROM test_table1 order by id limit 1", '333',settings={"password": pTe5Tb0s}, user=default)
    assert_eq_with_retry(node2, "SELECT id FROM test_table1 order by id limit 1", '333',settings={"password": dtnDvTr9}, user=default)

    node2.query("ALTER TABLE test_table1 DROP PARTITION 20170617")
    assert_eq_with_retry(node1, "SELECT id FROM test_table1 order by id limit 1", '555',settings={"password": pTe5Tb0s}, user=default)
    assert_eq_with_retry(node2, "SELECT id FROM test_table1 order by id limit 1", '555',settings={"password": dtnDvTr9}, user=default)


    node3.query("ALTER TABLE test_table2 DROP PARTITION 20170617")
    assert_eq_with_retry(node2, "SELECT id FROM test_table2 order by id limit 1", '555',settings={"password": azAUGBFl}, user=zhang)
    assert_eq_with_retry(node3, "SELECT id FROM test_table2 order by id limit 1", '555',settings={"password": ROgXGTDq}, user=default)

