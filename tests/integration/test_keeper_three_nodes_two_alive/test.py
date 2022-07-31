#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
from kazoo.client import KazooClient, KazooState

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml", "configs/keeper_conf.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml", "configs/keeper_conf.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml", "configs/keeper_conf.xml"],
    stay_alive=True,
)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def start(node):
    node.start_clickhouse()


def delete_with_retry(node_name, path):
    for _ in range(30):
        try:
            get_fake_zk(node_name).delete(path)
            return
        except:
            time.sleep(0.5)
    raise Exception(f"Cannot delete {path} from node {node_name}")


def test_start_offline(started_cluster):
    p = Pool(3)
    try:
        node1_zk = get_fake_zk("node1")
        node1_zk.create("/test_alive", b"aaaa")

        node1.stop_clickhouse()
        node2.stop_clickhouse()
        node3.stop_clickhouse()

        time.sleep(3)
        p.map(start, [node2, node3])

        assert node2.contains_in_log(
            "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start"
        )
        assert node3.contains_in_log(
            "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start"
        )

        node2_zk = get_fake_zk("node2")
        node2_zk.create("/c", b"data")

    finally:
        p.map(start, [node1, node2, node3])
        delete_with_retry("node1", "/test_alive")


def test_start_non_existing(started_cluster):
    p = Pool(3)
    try:
        node1.stop_clickhouse()
        node2.stop_clickhouse()
        node3.stop_clickhouse()

        node1.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
            "node3",
            "non_existing_node",
        )
        node2.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
            "node3",
            "non_existing_node",
        )

        time.sleep(3)
        p.map(start, [node2, node1])

        assert node1.contains_in_log(
            "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start"
        )
        assert node2.contains_in_log(
            "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start"
        )

        node2_zk = get_fake_zk("node2")
        node2_zk.create("/test_non_exising", b"data")
    finally:
        node1.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper1.xml",
            "non_existing_node",
            "node3",
        )
        node2.replace_in_config(
            "/etc/clickhouse-server/config.d/enable_keeper2.xml",
            "non_existing_node",
            "node3",
        )
        p.map(start, [node1, node2, node3])
        delete_with_retry("node2", "/test_non_exising")


def test_restart_third_node(started_cluster):
    node1_zk = get_fake_zk("node1")
    node1_zk.create("/test_restart", b"aaaa")

    node3.restart_clickhouse()

    assert node3.contains_in_log(
        "Connected to ZooKeeper (or Keeper) before internal Keeper start"
    )
    node1_zk.delete("/test_restart")
