import time
from multiprocessing.dummy import Pool

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', user_configs=['configs/user_restrictions.xml'])
node2 = cluster.add_instance('node2', user_configs=['configs/user_restrictions.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("create table nums (number UInt64) ENGINE = MergeTree() order by tuple()")
        node1.query("insert into nums values(0),(1)")

        yield cluster
    finally:
        cluster.shutdown()


def test_exception_message(started_cluster):
    assert node1.query("select number from nums order by number") == "0\n1\n"

    def node_busy(_):
        for i in range(10):
            node1.query("select sleep(2)", user='default')

    busy_pool = Pool(3)
    busy_pool.map_async(node_busy, range(3))
    time.sleep(1)  # wait a little until polling starts
    try:
        assert node2.query("select number from remote('node1', 'default', 'nums')", user='good') == "0\n1\n"
    except Exception as ex:
        print(ex.message)
        assert False, "Exception thrown while max_concurrent_queries_for_user is not exceeded"
