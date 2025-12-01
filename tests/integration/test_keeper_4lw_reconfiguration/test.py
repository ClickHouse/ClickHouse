#!/usr/bin/env python3

import csv
import re
import time
import json

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)
node4 = cluster.add_instance(
    "node4", main_configs=["configs/enable_keeper4.xml"], stay_alive=True
)
node5 = cluster.add_instance(
    "node5", main_configs=["configs/enable_keeper5.xml"], stay_alive=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_reconfig_option1(started_cluster):
    zk = keeper_utils.get_fake_zk(cluster, "node3", timeout=30)
    command = {
        "preconditions": {
           "leaders": [1, 2],
           "members": [1, 2, 3, 4, 5]
        },
        "actions": [
            {
                "transfer_leadership": [3]
            },
            {
                "remove_members": [1, 2]
            },
            {
                "set_priority": [{"id": 4, "priority": 100}, {"id": 5, "priority": 100}]
            },
            {
                "transfer_leadership": [4, 5]
            },
            {
                "set_priority": [{"id": 3, "priority": 0}]
            }
        ]
    }
    json_command = json.dumps(command)
    print(zk.get("/keeper/config"))
    print(keeper_utils.send_4lw_cmd(started_cluster, node3, cmd="rcfg", port=9181, argument=json_command))
    print(zk.get("/keeper/config"))
