#!/usr/bin/env python3


#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.test_tools import assert_eq_with_retry
from kazoo.client import KazooClient, KazooState

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
node4 = cluster.add_instance("node4", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def start(node):
    node.start_clickhouse()


def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_node_move(started_cluster):
    zk_conn = get_fake_zk(node1)

    for i in range(100):
        zk_conn.create("/test_four_" + str(i), b"somedata")

    zk_conn2 = get_fake_zk(node2)
    zk_conn2.sync("/test_four_0")

    zk_conn3 = get_fake_zk(node3)
    zk_conn3.sync("/test_four_0")

    for i in range(100):
        assert zk_conn2.exists("test_four_" + str(i)) is not None
        assert zk_conn3.exists("test_four_" + str(i)) is not None

    node4.stop_clickhouse()
    node4.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper_node4_4.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper4.xml",
    )
    p = Pool(3)
    waiter = p.apply_async(start, (node4,))
    node1.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper_node4_1.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper1.xml",
    )
    node2.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper_node4_2.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper2.xml",
    )

    node1.query("SYSTEM RELOAD CONFIG")
    node2.query("SYSTEM RELOAD CONFIG")

    waiter.wait()

    zk_conn4 = get_fake_zk(node4)
    zk_conn4.sync("/test_four_0")

    for i in range(100):
        assert zk_conn4.exists("/test_four_" + str(i)) is not None

    with pytest.raises(Exception):
        # Adding and removing nodes is async operation
        for i in range(10):
            zk_conn3 = get_fake_zk(node3)
            zk_conn3.sync("/test_four_0")
            time.sleep(i)
