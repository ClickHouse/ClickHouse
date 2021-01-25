import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_test_keeper1.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/enable_test_keeper2.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])
node3 = cluster.add_instance('node3', main_configs=['configs/enable_test_keeper3.xml', 'configs/log_conf.xml', 'configs/use_test_keeper.xml'])

from kazoo.client import KazooClient

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_simple_replicated_table(started_cluster):

    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE t (value UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t', '{}') ORDER BY tuple()".format(i + 1))

    node2.query("INSERT INTO t SELECT number FROM numbers(10)")

    node1.query("SYSTEM SYNC REPLICA t", timeout=10)
    node3.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT COUNT() FROM t") == "10\n"
    assert node2.query("SELECT COUNT() FROM t") == "10\n"
    assert node3.query("SELECT COUNT() FROM t") == "10\n"
