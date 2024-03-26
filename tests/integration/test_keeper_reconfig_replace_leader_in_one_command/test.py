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
