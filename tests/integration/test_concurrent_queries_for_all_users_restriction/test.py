import time
from multiprocessing.dummy import Pool

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', user_configs=['configs/user_restrictions.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("create table nums (number UInt64) ENGINE = MergeTree() order by tuple()")
        node1.query("insert into nums values (0), (1)")
        yield cluster
    finally:
        cluster.shutdown()


def test_exception_message(started_cluster):
    assert node1.query("select number from nums order by number") == "0\n1\n"

    def node_busy(_):
        for i in range(10):
            node1.query("select sleep(2)", user='someuser', ignore_error=True)

    busy_pool = Pool(3)
    busy_pool.map_async(node_busy, range(3))
    time.sleep(1)  # wait a little until polling starts

    with pytest.raises(Exception) as exc_info:
        for i in range(3):
            assert node1.query("select number from remote('node1', 'default', 'nums')", user='default') == "0\n1\n"
    exc_info.match("Too many simultaneous queries for all users")

    for i in range(3):
        assert node1.query("select number from remote('node1', 'default', 'nums')", user='default',
                           settings={'max_concurrent_queries_for_all_users': 0}) == "0\n1\n"
