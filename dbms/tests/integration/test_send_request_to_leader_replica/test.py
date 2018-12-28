import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_local_default.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_local_default.xml'], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()


        for node in [node1, node2]:
            node.query('''
            CREATE TABLE sometable(date Date, id UInt32, value Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/sometable', '{replica}', date, id, 8192);
                '''.format(replica=node.name), user='awesome')


        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("query,expected", [
    ("ALTER TABLE sometable DROP PARTITION 201706", '1'),
    ("TRUNCATE TABLE sometable", '0'),
    ("OPTIMIZE TABLE sometable", '4'),
])
def test_alter_table_drop_partition(started_cluster, query, expected):
    to_insert = '''\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
2017-07-16	444	3
'''
    node1.query("INSERT INTO sometable FORMAT TSV", stdin=to_insert, user='awesome')

    assert_eq_with_retry(node1, "SELECT COUNT(*) from sometable", '4', user='awesome')
    assert_eq_with_retry(node2, "SELECT COUNT(*) from sometable", '4', user='awesome')

    ### It maybe leader and everything will be ok
    node1.query(query, user='awesome')

    assert_eq_with_retry(node1, "SELECT COUNT(*) from sometable", expected, user='awesome')
    assert_eq_with_retry(node2, "SELECT COUNT(*) from sometable", expected, user='awesome')

    node1.query("INSERT INTO sometable FORMAT TSV", stdin=to_insert, user='awesome')

    assert_eq_with_retry(node1, "SELECT COUNT(*) from sometable", '4', user='awesome')
    assert_eq_with_retry(node2, "SELECT COUNT(*) from sometable", '4', user='awesome')

    ### If node1 is leader than node2 will be slave
    node2.query(query, user='awesome')

    assert_eq_with_retry(node1, "SELECT COUNT(*) from sometable", expected, user='awesome')
    assert_eq_with_retry(node2, "SELECT COUNT(*) from sometable", expected, user='awesome')
