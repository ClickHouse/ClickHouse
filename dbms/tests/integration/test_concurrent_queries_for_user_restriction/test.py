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

        node2.query("create table nums (number UInt64) ENGINE = MergeTree() order by tuple()")
        node2.query("insert into nums values(0),(1)")

        yield cluster
    finally:
        cluster.shutdown()

def test_exception_message(started_cluster):
    assert node1.query("select ID from distr_tbl order by ID") == "0\n1\n"
    assert node1.query("select number from nums order by number") == "0\n1\n"

    def node_busy(_):
        for i in xrange(10):
            node1.query("select sleep(2)", user='default')

    busy_pool = Pool(3)
    busy_pool.map_async(node_busy, xrange(3))
    time.sleep(1) # wait a little until polling start
    try:
        assert node2.query("select number from remote('node1', 'default', 'nums')", user='good') == "0\n1\n"
    except Exception as ex:
        print ex.message
        assert False, "Exception thrown while max_concurrent_queries_for_user is not exceeded"
