#!/usr/bin/env python3
import os
import subprocess
import typing as tp

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
node1 = cluster.add_instance("node1", main_configs=["configs/keeper1.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/keeper2.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/keeper3.xml"])

log_msg_removed = "has been removed from the cluster"
zk1, zk2, zk3 = None, None, None


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        conn: tp.Optional[ku.KeeperClient]
        for conn in [zk1, zk2, zk3]:
            if conn:
                conn.stop()

        cluster.shutdown()


def create_client(node: ClickHouseInstance):
    return ku.KeeperClient(
        cluster.server_bin_path, cluster.get_instance_ip(node.name), 9181
    )


def test_reconfig_remove_followers_from_3(started_cluster):
    """
    Remove 1 follower node from cluster of 3.
    Then remove another follower from two left nodes.
    Check that remaining node is in standalone mode.
    """

    global zk1, zk2, zk3
    zk1 = create_client(node1)
    config = zk1.get("/keeper/config")
    print("Initial config", config)

    assert len(config.split("\n")) == 3
    assert "node1" in config
    assert "node2" in config
    assert "node3" in config

    with pytest.raises(ValueError):
        zk1.reconfig(joining=None, leaving=None, new_members=None)
    with pytest.raises(ku.KeeperException):
        # bulk reconfiguration is not supported
        zk1.reconfig(joining=None, leaving=None, new_members="3")
    with pytest.raises(ValueError):
        zk1.reconfig(joining="1", leaving="1", new_members="3")
    with pytest.raises(ku.KeeperException):
        # at least one node must be left
        zk1.reconfig(joining=None, leaving="1,2,3", new_members=None)

    for i in range(100):
        zk1.create(f"/test_two_{i}", "somedata")

    zk2 = create_client(node2)
    zk2.sync("/test_two_0")
    ku.wait_configs_equal(config, zk2)

    zk3 = create_client(node3)
    zk3.sync("/test_two_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(100):
        assert zk2.exists(f"test_two_{i}")
        assert zk3.exists(f"test_two_{i}")

    config = zk1.reconfig(joining=None, leaving="3", new_members=None)
    print("After removing 3", config)

    assert len(config.split("\n")) == 2
    assert "node1" in config
    assert "node2" in config
    assert "node3" not in config

    zk2.stop()
    zk2 = create_client(node2)
    ku.wait_configs_equal(config, zk2)

    for i in range(100):
        assert zk2.exists(f"test_two_{i}")
        zk2.create(f"/test_two_{100 + i}", "otherdata")

    zk1.stop()
    zk1 = create_client(node1)
    zk1.sync("/test_two_0")

    for i in range(200):
        assert zk1.exists(f"test_two_{i}")

    assert node3.contains_in_log(log_msg_removed)

    for i in range(100):
        zk2.create(f"/test_two_{200 + i}", "otherdata")

    config = zk1.reconfig(joining=None, leaving="2", new_members=None)

    print("After removing 2", config)
    assert len(config.split("\n")) == 1
    assert "node1" in config
    assert "node2" not in config
    assert "node3" not in config

    zk1.stop()
    zk1 = create_client(node1)
    zk1.sync("/test_two_0")

    for i in range(300):
        assert zk1.exists(f"test_two_{i}")

    assert not node1.contains_in_log(log_msg_removed)
    assert node2.contains_in_log(log_msg_removed)
    assert "Mode: standalone" in zk1.execute_query("stat")
