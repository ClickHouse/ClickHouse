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
                conn.close()

        cluster.shutdown()


# can't use create_client as clickhouse-keeper-client 's reconfig doesn't support
# joining and adding in single reconfig command, thus duplication
# TODO myrrc this should be removed once keeper-client is updated


def get_fake_zk(node):
    return ku.get_fake_zk(cluster, node)


def get_config_str(zk):
    return ku.get_config_str(zk)[0].decode("utf-8")


def wait_configs_equal(
    left_config: str, right_zk: ku.KeeperClient, timeout: float = 30.0
):
    """
    Check whether get /keeper/config result in left_config is equal
    to get /keeper/config on right_zk ZK connection.
    """
    elapsed: float = 0.0
    while sorted(left_config.split("\n")) != sorted(
        get_config_str(right_zk).split("\n")
    ):
        time.sleep(1)
        elapsed += 1
        if elapsed >= timeout:
            raise Exception(
                f"timeout while checking nodes configs to get equal. "
                f"Left: {left_config}, right: {get_config_str(right_zk)}"
            )


def test_reconfig_replace_leader_in_one_command(started_cluster):
    """
    Remove leader from a cluster of 3 and add a new node to this cluster in a single command
    """

    zk1 = get_fake_zk(node1)
    config = get_config_str(zk1)

    assert len(config.split("\n")) == 3
    assert "node1" in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" not in config

    for i in range(100):
        zk1.create(f"/test_four_{i}", b"somedata")

    zk2 = get_fake_zk(node2)
    zk2.sync("/test_four_0")
    wait_configs_equal(config, zk2)

    zk3 = get_fake_zk(node3)
    zk3.sync("/test_four_0")
    wait_configs_equal(config, zk3)

    for i in range(100):
        assert zk2.exists(f"/test_four_{i}") is not None
        assert zk3.exists(f"/test_four_{i}") is not None

    assert ku.is_leader(cluster, node1)
    node4.start_clickhouse()
    config, _ = zk2.reconfig(
        joining="server.4=node4:9234", leaving="1", new_members=None
    )
    config = config.decode("utf-8")

    print("After removing 1 and adding 4", config)
    assert len(config.split("\n")) == 3
    assert "node1" not in config
    assert "node2" in config
    assert "node3" in config
    assert "node4" in config

    ku.wait_until_connected(cluster, node4)
    time.sleep(1)

    zk4 = get_fake_zk(node4)
    zk4.sync("/test_four_0")
    # we have an additional 20s timeout for removing leader
    wait_configs_equal(config, zk4, timeout=50)

    for i in range(100):
        assert zk4.exists(f"test_four_{i}") is not None
        zk4.create(f"/test_four_{100 + i}", b"somedata")

    with pytest.raises(Exception):
        zk1.stop()
        zk1.close()
        zk1 = get_fake_zk(node1)
        zk1.sync("/test_four_0")

    zk2.stop()
    zk2.close()
    zk2 = get_fake_zk(node2)
    zk2.sync("/test_four_0")
    wait_configs_equal(config, zk2)

    zk3.stop()
    zk3.close()
    zk3 = get_fake_zk(node3)
    zk3.sync("/test_four_0")
    wait_configs_equal(config, zk3)

    for i in range(200):
        assert zk2.exists(f"test_four_{i}") is not None
        assert zk3.exists(f"test_four_{i}") is not None
