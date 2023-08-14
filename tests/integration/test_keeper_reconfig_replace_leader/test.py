#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
from os.path import join, dirname, realpath
import time
import helpers.keeper_utils as ku
from kazoo.client import KazooClient, KazooState

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
        for conn in [zk1, zk2, zk3, zk4]:
            if conn:
                conn.stop()
                conn.close()

        cluster.shutdown()


def get_fake_zk(node):
    return ku.get_fake_zk(cluster, node)


def test_reconfig_replace_leader(started_cluster):
    """
    Remove leader from a cluster of 3 and add a new node via two commands.
    """

    zk1 = get_fake_zk(node1)
    config = ku.get_config_str(zk1)

    assert len(config.split("\n")) == 3
    assert "node1" in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" not in config

    for i in range(100):
        zk1.create(f"/test_four_{i}", b"somedata")

    zk2 = get_fake_zk(node2)
    zk2.sync("/test_four_0")
    ku.wait_configs_equal(config, zk2)

    zk3 = get_fake_zk(node3)
    zk3.sync("/test_four_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(100):
        assert zk2.exists(f"/test_four_{i}") is not None
        assert zk3.exists(f"/test_four_{i}") is not None

    assert ku.is_leader(cluster, node1)
    config, _ = zk2.reconfig(joining=None, leaving="1", new_members=None)
    config = config.decode("utf-8")

    print("After removing 1 (leader)", config)
    assert len(config.split("\n")) == 2
    assert "node1" not in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" not in config

    with pytest.raises(Exception):
        zk1.stop()
        zk1.close()
        zk1 = get_fake_zk(node1)
        zk1.sync("/test_four_0")

    node4.start_clickhouse()
    config, _ = zk2.reconfig(
        joining="server.4=node4:9234", leaving=None, new_members=None
    )
    config = config.decode("utf-8")
    ku.wait_until_connected(cluster, node4)

    print("After adding 4", config)
    assert len(config.split("\n")) == 3
    assert "node1" not in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" in config

    zk4 = get_fake_zk(node4)
    ku.wait_configs_equal(config, zk4)

    for i in range(100):
        assert zk4.exists(f"test_four_{i}") is not None
        zk4.create(f"/test_four_{100 + i}", b"somedata")

    zk2.stop()
    zk2.close()
    zk2 = get_fake_zk(node2)
    zk2.sync("/test_four_0")
    ku.wait_configs_equal(config, zk2)

    zk3.stop()
    zk3.close()
    zk3 = get_fake_zk(node3)
    zk3.sync("/test_four_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(200):
        assert zk2.exists(f"test_four_{i}") is not None
        assert zk3.exists(f"test_four_{i}") is not None
