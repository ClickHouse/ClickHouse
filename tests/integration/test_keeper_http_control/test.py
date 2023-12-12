#!/usr/bin/env python3

import os
import pytest
import requests

import helpers.keeper_utils as keeper_utils
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def test_http_readiness(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/ready".format(host=leader.ip_address, port=9182)
    )
    assert response.status_code == 200

    readiness_data = response.json()
    assert readiness_data["status"] == "ok"
    assert readiness_data["details"]["leader"] == True
    assert readiness_data["details"]["follower"] == False

    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/ready".format(host=follower.ip_address, port=9182)
    )
    assert response.status_code == 200

    readiness_data = response.json()
    assert readiness_data["status"] == "ok"
    assert readiness_data["details"]["leader"] == False
    assert readiness_data["details"]["follower"] == True
