#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as ku
import os
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionException, BadArgumentsException

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
        for conn in [zk1, zk2, zk3]:
            if conn:
                conn.stop()
                conn.close()

        cluster.shutdown()


def get_fake_zk(node):
    return ku.get_fake_zk(cluster, node)


def test_reconfig_remove_followers_from_3(started_cluster):
    """
    Remove 1 follower node from cluster of 3.
    Then remove another follower from two left nodes.
    Check that remaining node is in standalone mode.
    """

    zk1 = get_fake_zk(node1)
    config, _ = zk1.get("/keeper/config")
    config = config.decode("utf-8")
    print("Initial config", config)

    assert len(config.split("\n")) == 3
    assert "node1" in config
    assert "node2" in config
    assert "node3" in config

    with pytest.raises(BadVersionException):
        zk1.reconfig(joining=None, leaving="1", new_members=None, from_config=20)
    with pytest.raises(BadArgumentsException):
        zk1.reconfig(joining=None, leaving=None, new_members=None)
    with pytest.raises(BadArgumentsException):
        # bulk reconfiguration is not supported
        zk1.reconfig(joining=None, leaving=None, new_members="3")
    with pytest.raises(BadArgumentsException):
        zk1.reconfig(joining="1", leaving="1", new_members="3")
    with pytest.raises(BadArgumentsException):
        # at least one node must be left
        zk1.reconfig(joining=None, leaving="1,2,3", new_members=None)

    for i in range(100):
        zk1.create(f"/test_two_{i}", b"somedata")

    zk2 = get_fake_zk(node2)
    zk2.sync("/test_two_0")
    ku.wait_configs_equal(config, zk2)

    zk3 = get_fake_zk(node3)
    zk3.sync("/test_two_0")
    ku.wait_configs_equal(config, zk3)

    for i in range(100):
        assert zk2.exists(f"test_two_{i}") is not None
        assert zk3.exists(f"test_two_{i}") is not None

    config, _ = zk1.reconfig(joining=None, leaving="3", new_members=None)
    config = config.decode("utf-8")
    print("After removing 3", config)

    assert len(config.split("\n")) == 2
    assert "node1" in config
    assert "node2" in config
    assert "node3" not in config

    zk2.stop()
    zk2.close()
    zk2 = get_fake_zk(node2)
    ku.wait_configs_equal(config, zk2)

    for i in range(100):
        assert zk2.exists(f"test_two_{i}") is not None
        zk2.create(f"/test_two_{100 + i}", b"otherdata")

    zk1.stop()
    zk1.close()
    zk1 = get_fake_zk(node1)
    zk1.sync("/test_two_0")

    for i in range(200):
        assert zk1.exists(f"test_two_{i}") is not None

    with pytest.raises(Exception):
        zk3.stop()
        zk3.close()
        zk3 = get_fake_zk(node3)
        zk3.sync("/test_two_0")

    assert node3.contains_in_log(log_msg_removed)

    for i in range(100):
        zk2.create(f"/test_two_{200 + i}", b"otherdata")

    config, _ = zk1.reconfig(joining=None, leaving="2", new_members=None)
    config = config.decode("utf-8")

    print("After removing 2", config)
    assert len(config.split("\n")) == 1
    assert "node1" in config
    assert "node2" not in config
    assert "node3" not in config

    zk1.stop()
    zk1.close()
    zk1 = get_fake_zk(node1)
    zk1.sync("/test_two_0")

    for i in range(300):
        assert zk1.exists(f"test_two_{i}") is not None

    with pytest.raises(Exception):
        zk2.stop()
        zk2.close()
        zk2 = get_fake_zk(node2)
        zk2.sync("/test_two_0")

    assert not node1.contains_in_log(log_msg_removed)
    assert node2.contains_in_log(log_msg_removed)
    assert "Mode: standalone" in zk1.command(b"stat")
