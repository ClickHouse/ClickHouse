#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def test_recover_from_snapshot(started_cluster):
    try:
        node1_zk = node2_zk = node3_zk = None
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create("/test_snapshot_multinode_recover", "somedata".encode())

        node2_zk.sync("/test_snapshot_multinode_recover")
        node3_zk.sync("/test_snapshot_multinode_recover")

        assert node1_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"
        assert node2_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"
        assert node3_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"

        node3.stop_clickhouse(kill=True)

        # at least we will have 2 snapshots
        for i in range(435):
            node1_zk.create(
                "/test_snapshot_multinode_recover" + str(i),
                ("somedata" + str(i)).encode(),
            )

        for i in range(435):
            if i % 10 == 0:
                node1_zk.delete("/test_snapshot_multinode_recover" + str(i))

    finally:
        for zk in [node1_zk, node2_zk, node3_zk]:
            stop_zk(zk)

    # stale node should recover from leader's snapshot
    # with some sanitizers can start longer than 5 seconds
    node3.start_clickhouse(20)
    print("Restarted")

    try:
        node1_zk = node2_zk = node3_zk = None
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.sync("/test_snapshot_multinode_recover")
        node2_zk.sync("/test_snapshot_multinode_recover")
        node3_zk.sync("/test_snapshot_multinode_recover")

        assert node1_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"
        assert node2_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"
        assert node3_zk.get("/test_snapshot_multinode_recover")[0] == b"somedata"

        for i in range(435):
            if i % 10 != 0:
                assert (
                    node1_zk.get("/test_snapshot_multinode_recover" + str(i))[0]
                    == ("somedata" + str(i)).encode()
                )
                assert (
                    node2_zk.get("/test_snapshot_multinode_recover" + str(i))[0]
                    == ("somedata" + str(i)).encode()
                )
                assert (
                    node3_zk.get("/test_snapshot_multinode_recover" + str(i))[0]
                    == ("somedata" + str(i)).encode()
                )
            else:
                assert (
                    node1_zk.exists("/test_snapshot_multinode_recover" + str(i)) is None
                )
                assert (
                    node2_zk.exists("/test_snapshot_multinode_recover" + str(i)) is None
                )
                assert (
                    node3_zk.exists("/test_snapshot_multinode_recover" + str(i)) is None
                )
    finally:
        for zk in [node1_zk, node2_zk, node3_zk]:
            stop_zk(zk)
