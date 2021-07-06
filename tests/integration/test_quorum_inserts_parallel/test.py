#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
node3 = cluster.add_instance("node3", with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_parallel_quorum_actually_parallel(started_cluster):
    settings = {"insert_quorum": "3", "insert_quorum_parallel": "1"}
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '{num}') ORDER BY tuple()".format(num=i))

    p = Pool(10)

    def long_insert(node):
        node.query("INSERT INTO r SELECT number, toString(number) FROM numbers(5) where sleepEachRow(1) == 0", settings=settings)

    job = p.apply_async(long_insert, (node1,))

    node2.query("INSERT INTO r VALUES (6, '6')", settings=settings)
    assert node1.query("SELECT COUNT() FROM r") == "1\n"
    assert node2.query("SELECT COUNT() FROM r") == "1\n"
    assert node3.query("SELECT COUNT() FROM r") == "1\n"

    node1.query("INSERT INTO r VALUES (7, '7')", settings=settings)
    assert node1.query("SELECT COUNT() FROM r") == "2\n"
    assert node2.query("SELECT COUNT() FROM r") == "2\n"
    assert node3.query("SELECT COUNT() FROM r") == "2\n"

    job.get()

    assert node1.query("SELECT COUNT() FROM r") == "7\n"
    assert node2.query("SELECT COUNT() FROM r") == "7\n"
    assert node3.query("SELECT COUNT() FROM r") == "7\n"
    p.close()
    p.join()


def test_parallel_quorum_actually_quorum(started_cluster):
    for i, node in enumerate([node1, node2, node3]):
        node.query("CREATE TABLE q (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/q', '{num}') ORDER BY tuple()".format(num=i))

    with PartitionManager() as pm:
        pm.partition_instances(node2, node1, port=9009)
        pm.partition_instances(node2, node3, port=9009)
        with pytest.raises(QueryRuntimeException):
            node1.query("INSERT INTO q VALUES(1, 'Hello')", settings={"insert_quorum": "3", "insert_quorum_parallel": "1", "insert_quorum_timeout": "3000"})

        assert_eq_with_retry(node1, "SELECT COUNT() FROM q", "1")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM q", "0")
        assert_eq_with_retry(node3, "SELECT COUNT() FROM q", "1")

        node1.query("INSERT INTO q VALUES(2, 'wlrd')", settings={"insert_quorum": "2", "insert_quorum_parallel": "1", "insert_quorum_timeout": "3000"})

        assert_eq_with_retry(node1, "SELECT COUNT() FROM q", "2")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM q", "0")
        assert_eq_with_retry(node3, "SELECT COUNT() FROM q", "2")

        def insert_value_to_node(node, settings):
            node.query("INSERT INTO q VALUES(3, 'Hi')", settings=settings)

        p = Pool(2)
        res = p.apply_async(insert_value_to_node, (node1, {"insert_quorum": "3", "insert_quorum_parallel": "1", "insert_quorum_timeout": "60000"}))

        assert_eq_with_retry(node1, "SELECT COUNT() FROM system.parts WHERE table == 'q' and active == 1", "3")
        assert_eq_with_retry(node3, "SELECT COUNT() FROM system.parts WHERE table == 'q' and active == 1", "3")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM system.parts WHERE table == 'q' and active == 1", "0")

        # Insert to the second to satisfy quorum
        insert_value_to_node(node2, {"insert_quorum": "3", "insert_quorum_parallel": "1"})

        res.get()

        assert_eq_with_retry(node1, "SELECT COUNT() FROM q", "3")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM q", "1")
        assert_eq_with_retry(node3, "SELECT COUNT() FROM q", "3")

        p.close()
        p.join()

    node2.query("SYSTEM SYNC REPLICA q", timeout=10)
    assert_eq_with_retry(node2, "SELECT COUNT() FROM q", "3")
