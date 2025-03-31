#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/enable_secure_keeper1.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/rootCA.pem",
    ],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/enable_secure_keeper2.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/rootCA.pem",
    ],
)
node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/enable_secure_keeper3.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/rootCA.pem",
    ],
)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_secure_raft_works(started_cluster):
    try:
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create("/test_node", b"somedata1")
        node2_zk.sync("/test_node")
        node3_zk.sync("/test_node")

        assert node1_zk.exists("/test_node") is not None
        assert node2_zk.exists("/test_node") is not None
        assert node3_zk.exists("/test_node") is not None
    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass
