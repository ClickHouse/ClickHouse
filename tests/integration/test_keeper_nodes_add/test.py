#!/usr/bin/env python3

import os
import time
from multiprocessing.dummy import Pool

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance("node2", main_configs=[], stay_alive=True)
node3 = cluster.add_instance("node3", main_configs=[], stay_alive=True)


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


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
            # Cleanup existing znodes from previous runs
            if zk_conn.exists("/test_two_" + str(i)):
                zk_conn.delete("/test_two_" + str(i))
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
            # Cleanup existing znodes from previous runs
            if zk_conn.exists("/test_three_" + str(i)):
                zk_conn.delete("/test_three_" + str(i))
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


def test_nodes_add_respect_priority(started_cluster):
    zk_conn = None

    try:
        node1.stop_clickhouse()
        node2.stop_clickhouse()
        node3.stop_clickhouse()

        for node in (node1, node2, node3):
            node.exec_in_container(
                ["bash", "-c", "rm -rf /var/lib/clickhouse/coordination"]
            )

        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )
        node1.start_clickhouse()
        keeper_utils.wait_until_connected(cluster, node1)
        zk_conn = get_fake_zk(node1)

        # Verify initial config has only node1
        initial_config = zk_conn.get("/keeper/config")[0].decode()
        assert "server.1=" in initial_config
        assert "server.2=" not in initial_config
        assert "server.3=" not in initial_config

        p = Pool(3)

        node2.stop_clickhouse()
        node2.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_with_priority_2.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
        )
        node3.stop_clickhouse()
        node3.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_with_priority_3.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper3.xml",
        )

        waiter2 = p.apply_async(start, (node2,))
        waiter3 = p.apply_async(start, (node3,))

        # Update node1 config to add node2 (priority 90) and node3 (priority 80)
        node1.copy_file_to_container(
            os.path.join(CONFIG_DIR, "enable_keeper_three_nodes_with_priority_1.xml"),
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
        )
        # Monitor config changes by collecting raw configs
        configs = []
        start_time = time.time()
        timeout = 30.0
        poll_interval = 0.1

        while time.time() - start_time < timeout:
            try:
                config = zk_conn.get("/keeper/config")[0].decode()

                # Record config if it changed
                if not configs or configs[-1] != config:
                    configs.append(config)

                # Stop when both servers appear
                if "server.2=" in config and "server.3=" in config:
                    break

                time.sleep(poll_interval)
            except Exception:
                # Config might be temporarily unavailable during update
                time.sleep(poll_interval)

        waiter2.wait()
        waiter3.wait()

        assert len(configs) == 3, (
            f"Expected exactly 3 config states, got {len(configs)}. "
            f"Config sequence:\n" + "\n---\n".join(configs)
        )

        # initial state with only server.1
        assert "server.1=" in configs[0]
        assert "server.2=" not in configs[0]
        assert "server.3=" not in configs[0]

        # server.2 added (higher priority)
        assert "server.1=" in configs[1]
        assert "server.2=" in configs[1]
        assert "server.3=" not in configs[1]

        # server.3 added (lower priority)
        assert "server.1=" in configs[2]
        assert "server.2=" in configs[2]
        assert "server.3=" in configs[2]
    finally:
        if zk_conn:
            zk_conn.stop()
            zk_conn.close()
