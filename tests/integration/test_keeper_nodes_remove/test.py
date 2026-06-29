#!/usr/bin/env python3

import os
import time
import uuid

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


@pytest.fixture(scope="function")
def started_cluster():
    cluster = None
    try:
        run_uuid = uuid.uuid4()
        cluster = ClickHouseCluster(__file__, str(run_uuid))
        # Disable `with_remote_database_disk` as the test does not use the default Keeper.
        cluster.add_instance(
            "node1",
            main_configs=["configs/enable_keeper1.xml"],
            stay_alive=True,
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

        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(cluster, node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def test_nodes_remove(started_cluster):
    zk_conn = None
    zk_conn2 = None
    zk_conn3 = None

    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]
    node3 = started_cluster.instances["node3"]

    try:
        zk_conn = get_fake_zk(started_cluster, node1)

        for i in range(100):
            if zk_conn.exists("/test_two_" + str(i)):
                zk_conn.delete("/test_two_" + str(i))
            zk_conn.create("/test_two_" + str(i), b"somedata")

        zk_conn2 = get_fake_zk(started_cluster, node2)
        zk_conn2.sync("/test_two_0")

        zk_conn3 = get_fake_zk(started_cluster, node3)
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
        zk_conn2 = get_fake_zk(started_cluster, node2)

        for i in range(100):
            assert zk_conn2.exists("test_two_" + str(i)) is not None
            if zk_conn2.exists("/test_two_" + str(100 + i)):
                zk_conn2.delete("/test_two_" + str(100 + i))
            zk_conn2.create("/test_two_" + str(100 + i), b"otherdata")

        zk_conn.stop()
        zk_conn.close()
        zk_conn = get_fake_zk(started_cluster, node1)
        zk_conn.sync("/test_two_0")

        for i in range(100):
            assert zk_conn.exists("test_two_" + str(i)) is not None
            assert zk_conn.exists("test_two_" + str(100 + i)) is not None

        try:
            zk_conn3.stop()
            zk_conn3.close()
            zk_conn3 = get_fake_zk(started_cluster, node3)
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
        zk_conn = get_fake_zk(started_cluster, node1)
        zk_conn.sync("/test_two_0")

        for i in range(100):
            assert zk_conn.exists("test_two_" + str(i)) is not None
            assert zk_conn.exists("test_two_" + str(100 + i)) is not None

        try:
            zk_conn2.stop()
            zk_conn2.close()
            zk_conn2 = get_fake_zk(started_cluster, node2)
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
