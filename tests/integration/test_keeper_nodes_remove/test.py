#!/usr/bin/env python3

import os
import time

import pytest
from kazoo.client import KazooClient, KazooState

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_nodes_remove(started_cluster):
    zk_conn = None
    zk_conn2 = None
    zk_conn3 = None

    try:
        zk_conn = get_fake_zk(node1)

        for i in range(100):
            zk_conn.create("/test_two_" + str(i), b"somedata")

        zk_conn2 = get_fake_zk(node2)
        zk_conn2.sync("/test_two_0")

        zk_conn3 = get_fake_zk(node3)
        zk_conn3.sync("/test_two_0")

        for i in range(100):
            assert zk_conn2.exists("test_two_" + str(i)) is not None
            assert zk_conn3.exists("test_two_" + str(i)) is not None

        node2.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_2.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
        )
        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        node1.query("SYSTEM RELOAD CONFIG")
        node2.query("SYSTEM RELOAD CONFIG")

        zk_conn2.stop()
        zk_conn2.close()
        zk_conn2 = get_fake_zk(node2)

        for i in range(100):
            assert zk_conn2.exists("test_two_" + str(i)) is not None
            zk_conn2.create("/test_two_" + str(100 + i), b"otherdata")

        zk_conn.stop()
        zk_conn.close()
        zk_conn = get_fake_zk(node1)
        zk_conn.sync("/test_two_0")

        for i in range(100):
            assert zk_conn.exists("test_two_" + str(i)) is not None
            assert zk_conn.exists("test_two_" + str(100 + i)) is not None

        try:
            zk_conn3.stop()
            zk_conn3.close()
            zk_conn3 = get_fake_zk(node3)
            zk_conn3.sync("/test_two_0")
            time.sleep(0.1)
        except Exception:
            pass

        node3.stop_clickhouse()

        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_single_keeper1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        node1.query("SYSTEM RELOAD CONFIG")

        zk_conn.stop()
        zk_conn.close()
        zk_conn = get_fake_zk(node1)
        zk_conn.sync("/test_two_0")

        for i in range(100):
            assert zk_conn.exists("test_two_" + str(i)) is not None
            assert zk_conn.exists("test_two_" + str(100 + i)) is not None

        try:
            zk_conn2.stop()
            zk_conn2.close()
            zk_conn2 = get_fake_zk(node2)
            zk_conn2.sync("/test_two_0")
            time.sleep(0.1)
        except Exception:
            pass

        node2.stop_clickhouse()
    finally:
        for zk in [zk_conn, zk_conn2, zk_conn3]:
            if zk:
                zk.stop()
                zk.close()
