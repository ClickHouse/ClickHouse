#!/usr/bin/env python3

import os
import random
import string
import time
from multiprocessing.dummy import Pool

import pytest
from kazoo.client import KazooClient, KazooState

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


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
