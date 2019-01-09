import time

import pytest

from multiprocessing.dummy import Pool
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', user_configs=['configs/user_restrictions.xml'], main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', user_configs=['configs/user_restrictions.xml'], main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for num, node in enumerate([node1, node2]):
            node.query("create table real_tbl (ID UInt64, Value String) ENGINE = MergeTree() order by tuple()")
            node.query("insert into real_tbl values(0, '0000'), (1, '1111')")
            node.query("create table distr_tbl (ID UInt64, Value String) ENGINE Distributed(test_cluster, default, real_tbl)")

        node1.query("create table nums (number UInt64) ENGINE = MergeTree() order by tuple()")
        node1.query("insert into nums values(0),(1)")

        yield cluster
    finally:
        cluster.shutdown()

def num_getter(num):
    if num % 2 == 0:
        return node1
    else:
        return node2

@pytest.mark.parametrize("node_getter", [
    (lambda _: node1),
    (lambda _: node2),
    (num_getter),
])
def test_exception_message(started_cluster, node_getter):
    assert node1.query("select ID from distr_tbl order by ID") == "0\n1\n"
    assert node1.query("select number from nums order by number") == "0\n1\n"
    try:
        p = Pool(10)
        def query(num):
            node = node_getter(num)
            node.query(
                    "select sleep(2) from distr_tbl where ID GLOBAL IN (select number from remote('node1', 'default', 'nums'))",
                    user='good')

        p.map(query, xrange(3))
    except Exception as ex:
        assert 'Too many simultaneous queries for user good.' in ex.message
        print ex.message
