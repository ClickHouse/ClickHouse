#!/usr/bin/env python3

from helpers.cluster import ClickHouseCluster
import pytest
import random
import string
import os
import time
from io import StringIO
import socket
import threading

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


def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 9181))
    return client


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


def send_4lw_cmd(node_name, cmd="ruok"):
    client = None
    try:
        client = get_keeper_socket(node_name)
        client.send(cmd.encode())
        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()


def test_aggressive_mntr(started_cluster):
    def go_mntr(node_name):
        for _ in range(100000):
            print(node_name, send_4lw_cmd(node_name, "mntr"))

    node1_thread = threading.Thread(target=lambda: go_mntr(node1.name))
    node2_thread = threading.Thread(target=lambda: go_mntr(node2.name))
    node3_thread = threading.Thread(target=lambda: go_mntr(node3.name))
    node1_thread.start()
    node2_thread.start()
    node3_thread.start()

    node2.stop_clickhouse()
    node3.stop_clickhouse()

    while send_4lw_cmd(node1.name, "mntr") != NOT_SERVING_REQUESTS_ERROR_MSG:
        time.sleep(0.2)

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
