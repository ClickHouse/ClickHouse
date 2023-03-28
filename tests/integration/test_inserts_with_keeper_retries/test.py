#!/usr/bin/env python3

import pytest
import time
import threading
from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_replica_inserts_with_keeper_restart(started_cluster):
    try:
        node1.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '0') ORDER BY tuple()"
        )

        p = Pool(1)
        zk_stopped_event = threading.Event()

        def zoo_restart(zk_stopped_event):
            cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
            zk_stopped_event.set()
            cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        job = p.apply_async(zoo_restart, (zk_stopped_event,))

        zk_stopped_event.wait(90)

        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=20"
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10) SETTINGS insert_keeper_max_retries=20"
        )

        job.wait()
        p.close()
        p.join()

        assert node1.query("SELECT COUNT() FROM r") == "20\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r SYNC")


def test_replica_inserts_with_keeper_disconnect(started_cluster):
    try:
        node1.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '0') ORDER BY tuple()"
        )

        p = Pool(1)
        disconnect_event = threading.Event()

        def keeper_disconnect(node, event):
            with PartitionManager() as pm:
                pm.drop_instance_zk_connections(node)
                event.set()

        job = p.apply_async(
            keeper_disconnect,
            (
                node1,
                disconnect_event,
            ),
        )
        disconnect_event.wait(90)

        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=20"
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10) SETTINGS insert_keeper_max_retries=20"
        )

        job.wait()
        p.close()
        p.join()

        assert node1.query("SELECT COUNT() FROM r") == "20\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r SYNC")


def test_query_timeout_with_zk_down(started_cluster):
    try:
        node1.query(
            "CREATE TABLE zk_down (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/zk_down', '0') ORDER BY tuple()"
        )

        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        start_time = time.time()
        with pytest.raises(QueryRuntimeException):
            node1.query(
                "INSERT INTO zk_down SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=10000, insert_keeper_retry_max_backoff_ms=1000, max_execution_time=1"
            )
        finish_time = time.time()
        assert finish_time - start_time < 10
    finally:
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        node1.query("DROP TABLE IF EXISTS zk_down SYNC")
