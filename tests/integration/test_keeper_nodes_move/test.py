#!/usr/bin/env python3

import os
import time
from multiprocessing.dummy import Pool
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


@pytest.fixture(scope="function")
def started_cluster():
    cluster = None
    try:
        cluster = ClickHouseCluster(__file__, str(uuid.uuid4()))

        # Disable `with_remote_database_disk` as the test does not use the default Keeper.
        cluster.add_instance(
            "node1",
            main_configs=["configs/enable_keeper1.xml"],
            stay_alive=True,
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/enable_keeper2.xml"],
            stay_alive=True,
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/enable_keeper3.xml"],
            stay_alive=True,
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node4",
            stay_alive=True,
            with_remote_database_disk=False,
        )

        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def start(cluster, node):
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


def get_fake_zk(cluster, node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def test_node_move(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]
    node3 = started_cluster.instances["node3"]
    node4 = started_cluster.instances["node4"]

    zk_conn = None
    zk_conn2 = None
    zk_conn3 = None
    zk_conn4 = None

    try:
        zk_conn = get_fake_zk(started_cluster, node1)

        for i in range(100):
            if zk_conn.exists("/test_four_" + str(i)):
                zk_conn.delete("/test_four_" + str(i))
            zk_conn.create("/test_four_" + str(i), b"somedata")

        zk_conn2 = get_fake_zk(started_cluster, node2)
        zk_conn2.sync("/test_four_0")

        zk_conn3 = get_fake_zk(started_cluster, node3)
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
        waiter = p.apply_async(start, (started_cluster, node4))
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

        zk_conn4 = get_fake_zk(started_cluster, node4)
        zk_conn4.sync("/test_four_0")

        for i in range(100):
            assert zk_conn4.exists("/test_four_" + str(i)) is not None

        with pytest.raises(Exception):
            # Adding and removing nodes is async operation
            for i in range(10):
                zk_conn3 = get_fake_zk(started_cluster, node3)
                zk_conn3.sync("/test_four_0")
                time.sleep(i)
    finally:
        for zk in [zk_conn, zk_conn2, zk_conn3, zk_conn4]:
            if zk:
                zk.stop()
                zk.close()
