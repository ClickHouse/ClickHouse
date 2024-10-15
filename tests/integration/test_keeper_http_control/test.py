#!/usr/bin/env python3

import os

import pytest
import requests

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

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


def test_http_readiness_basic_responses(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/ready".format(host=leader.ip_address, port=9182)
    )
    assert response.status_code == 200

    readiness_data = response.json()
    assert readiness_data["status"] == "ok"
    assert readiness_data["details"]["role"] == "leader"

    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/ready".format(host=follower.ip_address, port=9182)
    )
    assert response.status_code == 200

    readiness_data = response.json()
    assert readiness_data["status"] == "ok"
    assert readiness_data["details"]["role"] == "follower"
    assert readiness_data["details"]["hasLeader"] == True


def test_http_readiness_partitioned_cluster(started_cluster):
    with PartitionManager() as pm:
        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])

        pm.partition_instances(leader, follower)
        keeper_utils.wait_until_quorum_lost(cluster, follower)

        response = requests.get(
            "http://{host}:{port}/ready".format(host=follower.ip_address, port=9182)
        )
        print(response.json())
        assert response.status_code == 503

        readiness_data = response.json()
        assert readiness_data["status"] == "fail"
        assert readiness_data["details"]["role"] == "follower"
        assert readiness_data["details"]["hasLeader"] == False
