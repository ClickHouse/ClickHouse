#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
# from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry
import time


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
# node3 = cluster.add_instance("node3", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_replica_inserts_with_keeper_disconnect(started_cluster):
    settings = {"insert_quorum": "2", "insert_quorum_parallel": "0"}
    node1.query(
        "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '0') ORDER BY tuple()"
    )
    node2.query(
        "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '1') ORDER BY tuple()"
    )

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)

    node1.query(
        "INSERT INTO r SELECT number, toString(number) FROM numbers(10)",
        settings=settings,
    )
    node1.query(
        "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10)",
        settings=settings,
    )


    assert node1.query("SELECT COUNT() FROM r") == "20\n"
    assert node2.query("SELECT COUNT() FROM r") == "20\n"
