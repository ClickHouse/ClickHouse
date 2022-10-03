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
node2 = cluster.add_instance("node2", with_zookeeper=True)


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
        settings = {
            "insert_quorum": "2",
        }
        node1.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '0') ORDER BY tuple()"
        )
        node2.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '1') ORDER BY tuple()"
        )

        p = Pool(3)
        zk_stopped_event = threading.Event()

        def zoo_restart(zk_stopped_event):
            cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
            zk_stopped_event.set()
            cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        job = p.apply_async(zoo_restart, (zk_stopped_event,))

        zk_stopped_event.wait(60)

        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10)",
            settings=settings,
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10)",
            settings=settings,
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(20, 10)",
            settings=settings,
        )

        job.wait()
        p.close()
        p.join()

        assert node1.query("SELECT COUNT() FROM r") == "30\n"
        assert node2.query("SELECT COUNT() FROM r") == "30\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r SYNC")
        node2.query("DROP TABLE IF EXISTS r SYNC")


@pytest.mark.skip(reason="Unfortunately it showed to be flaky. Disabled for now")
def test_replica_inserts_with_keeper_disconnect(started_cluster):
    try:
        settings = {
            "insert_quorum": "2",
        }
        node1.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '0') ORDER BY tuple()"
        )
        node2.query(
            "CREATE TABLE r (a UInt64, b String) ENGINE=ReplicatedMergeTree('/test/r', '1') ORDER BY tuple()"
        )

        p = Pool(1)
        disconnect_event = threading.Event()

        def keeper_disconnect(node, event):
            with PartitionManager() as pm:
                pm.drop_instance_zk_connections(node)
                event.set()
                time.sleep(5)

        job = p.apply_async(
            keeper_disconnect,
            (
                node1,
                disconnect_event,
            ),
        )
        disconnect_event.wait(60)

        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10)",
            settings=settings,
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10)",
            settings=settings,
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(20, 10)",
            settings=settings,
        )

        assert node1.query("SELECT COUNT() FROM r") == "30\n"
        assert node2.query("SELECT COUNT() FROM r") == "30\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r SYNC")
        node2.query("DROP TABLE IF EXISTS r SYNC")
