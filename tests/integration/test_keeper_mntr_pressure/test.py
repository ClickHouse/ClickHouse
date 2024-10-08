#!/usr/bin/env python3

import os
import random
import socket
import string
import threading
import time
from io import StringIO

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["config/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["config/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["config/enable_keeper3.xml"], stay_alive=True
)

NOT_SERVING_REQUESTS_ERROR_MSG = "This instance is not currently serving requests"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


def test_aggressive_mntr(started_cluster):
    def go_mntr(node):
        for _ in range(10000):
            try:
                print(node.name, keeper_utils.send_4lw_cmd(cluster, node, "mntr"))
            except ConnectionRefusedError:
                pass

    node1_thread = threading.Thread(target=lambda: go_mntr(node1))
    node2_thread = threading.Thread(target=lambda: go_mntr(node2))
    node3_thread = threading.Thread(target=lambda: go_mntr(node3))
    node1_thread.start()
    node2_thread.start()
    node3_thread.start()

    node2.stop_clickhouse()
    node3.stop_clickhouse()

    keeper_utils.wait_until_quorum_lost(cluster, node1)

    node1.stop_clickhouse()
    starters = []
    for node in [node1, node2, node3]:
        start_thread = threading.Thread(target=lambda: node.start_clickhouse())
        start_thread.start()
        starters.append(start_thread)

    for start_thread in starters:
        start_thread.join()

    node1_thread.join()
    node2_thread.join()
    node3_thread.join()

    for node in [node1, node2, node3]:
        assert not node.contains_in_log("LOGICAL_ERROR")
