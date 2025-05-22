#!/usr/bin/env python3

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_smoke():
    node1_zk = None

    try:
        cluster.start()

        node1_zk = get_fake_zk("node1")
        node1_zk.create("/test_alive", b"aaaa")

    finally:
        cluster.shutdown()

        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
