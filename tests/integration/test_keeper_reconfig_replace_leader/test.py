#!/usr/bin/env python3

import time
import typing as tp
from os.path import dirname, join, realpath

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = join(dirname(realpath(__file__)), "configs")

node1 = cluster.add_instance("node1", main_configs=["configs/keeper1.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/keeper2.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/keeper3.xml"])
node4 = cluster.add_instance("node4", stay_alive=True)
zk1, zk2, zk3, zk4 = None, None, None, None


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node4.stop_clickhouse()
        node4.copy_file_to_container(
            join(CONFIG_DIR, "keeper4.xml"),
            "/etc/clickhouse-server/config.d/keeper.xml",
        )

        yield cluster

    finally:
        conn: tp.Optional[ku.KeeperClient]
        for conn in [zk1, zk2, zk3, zk4]:
            if conn:
                conn.stop()

        cluster.shutdown()


def create_client(node: ClickHouseInstance):
    return ku.KeeperClient(
        cluster.server_bin_path, cluster.get_instance_ip(node.name), 9181
    )


def test_reconfig_replace_leader(started_cluster):
    """
    Remove leader from a cluster of 3 and add a new node via two commands.
    """
    global zk1, zk2, zk3, zk4
    zk1 = create_client(node1)
    config = ku.get_config_str(zk1)

    assert len(config.split("\n")) == 3
    assert "node1" in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" not in config

    for i in range(100):
        zk1.create(f"/test_four_{i}", "somedata")

    zk2 = create_client(node2)
    zk2.sync("/test_four_0")
    ku.wait_configs_equal(config, zk2)

    zk3 = create_client(node3)
    zk3.sync("/test_four_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(100):
        assert zk2.exists(f"/test_four_{i}")
        assert zk3.exists(f"/test_four_{i}")

    assert ku.is_leader(cluster, node1)
    config = zk2.reconfig(joining=None, leaving="1", new_members=None)

    print("After removing 1 (leader)", config)
    assert len(config.split("\n")) == 2
    assert "node1" not in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" not in config

    # wait until cluster stabilizes with a new leader
    while not ku.is_leader(started_cluster, node2) and not ku.is_leader(
        started_cluster, node3
    ):
        time.sleep(1)

    # additional 20s wait before removing leader
    ku.wait_configs_equal(config, zk2, timeout=50)

    node4.start_clickhouse()
    config = zk2.reconfig(joining="server.4=node4:9234", leaving=None, new_members=None)
    ku.wait_until_connected(cluster, node4)

    print("After adding 4", config)
    assert len(config.split("\n")) == 3
    assert "node1" not in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" in config

    zk4 = create_client(node4)
    ku.wait_configs_equal(config, zk4)

    for i in range(100):
        assert zk4.exists(f"test_four_{i}")
        zk4.create(f"/test_four_{100 + i}", "somedata")

    zk2.stop()
    zk2 = create_client(node2)
    zk2.sync("/test_four_0")
    ku.wait_configs_equal(config, zk2)

    zk3.stop()
    zk3 = create_client(node3)
    zk3.sync("/test_four_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(200):
        assert zk2.exists(f"test_four_{i}") is not None
        assert zk3.exists(f"test_four_{i}") is not None
