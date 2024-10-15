#!/usr/bin/env python3

import os
import random
import string
import time
from multiprocessing.dummy import Pool

import pytest
from kazoo.client import KazooClient, KazooState

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance("node2", main_configs=[], stay_alive=True)
node3 = cluster.add_instance("node3", main_configs=[], stay_alive=True)


def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def start(node):
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


def test_nodes_add(started_cluster):
    zk_conn = None
    zk_conn2 = None
    zk_conn3 = None

    try:
        keeper_utils.wait_until_connected(cluster, node1)
        zk_conn = get_fake_zk(node1)

        for i in range(100):
            zk_conn.create("/test_two_" + str(i), b"somedata")

        p = Pool(3)
        node2.stop_clickhouse()
        node2.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_2.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
        )
        waiter = p.apply_async(start, (node2,))
        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_two_nodes_1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )
        node1.query("SYSTEM RELOAD CONFIG")
        waiter.wait()
        keeper_utils.wait_until_connected(cluster, node2)

        zk_conn2 = get_fake_zk(node2)

        for i in range(100):
            assert zk_conn2.exists("/test_two_" + str(i)) is not None

        zk_conn.stop()
        zk_conn.close()

        zk_conn = get_fake_zk(node1)

        for i in range(100):
            zk_conn.create("/test_three_" + str(i), b"somedata")

        node3.stop_clickhouse()

        node3.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_3.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper3.xml",
        )
        waiter = p.apply_async(start, (node3,))
        node2.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_2.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
        )
        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )

        node1.query("SYSTEM RELOAD CONFIG")
        node2.query("SYSTEM RELOAD CONFIG")

        waiter.wait()
        keeper_utils.wait_until_connected(cluster, node3)
        zk_conn3 = get_fake_zk(node3)

        for i in range(100):
            assert zk_conn3.exists("/test_three_" + str(i)) is not None

        # configs which change endpoints of server should not be allowed
        node1.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
            "node3",
            "non_existing_node",
        )

        node1.query("SYSTEM RELOAD CONFIG")
        time.sleep(2)
        assert node1.contains_in_log(
            "Config will be ignored because a server with ID 3 is already present in the cluster"
        )
    finally:
        for zk in [zk_conn, zk_conn2, zk_conn3]:
            if zk:
                zk.stop()
                zk.close()
