#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
from kazoo.client import KazooClient, KazooState

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


def test_nodes_add(started_cluster):
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

    zk_conn2 = get_fake_zk(node2)

    for i in range(100):
        assert zk_conn2.exists("/test_two_" + str(i)) is not None

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
    zk_conn3 = get_fake_zk(node3)

    for i in range(100):
        assert zk_conn3.exists("/test_three_" + str(i)) is not None
