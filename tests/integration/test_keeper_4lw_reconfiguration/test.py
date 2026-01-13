#!/usr/bin/env python3

import csv
import re
import time
import json

import pytest
import os

from multiprocessing.dummy import Pool
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
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
node6 = cluster.add_instance(
    "node6", stay_alive=True
)
node7 = cluster.add_instance(
    "node7", stay_alive=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def five_to_three_reconfig(started_cluster):
    zk = keeper_utils.get_fake_zk(cluster, "node3", timeout=30)
    command = {
        "max_action_wait_time_ms": 180000,
        "max_total_wait_time_ms": 600000,
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
    print(json_command)
    print(zk.get("/keeper/config"))

    result_str = keeper_utils.send_4lw_cmd(started_cluster, node3, cmd="rcfg", port=9181, argument=json_command, timeout_sec=180)
    print("Result:", result_str)
    result = json.loads(result_str)
    assert result["status"] == "ok"

    leader = keeper_utils.get_leader(started_cluster, [node3, node4, node5])
    assert leader.name in ["node4", "node5"]

    content = zk.get("/keeper/config")[0].decode("utf-8")
    assert 'server.1' not in content
    assert 'server.2' not in content

    assert 'server.3=node3:9234;participant;0' in content
    assert 'server.4=node4:9234;participant;100' in content
    assert 'server.5=node5:9234;participant;100' in content


def start(node):
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)

def three_to_five_reconfig(started_cluster):
    zk = keeper_utils.get_fake_zk(cluster, "node3", timeout=30)

    node6.stop_clickhouse()
    node6.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper6.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper6.xml",
    )

    node7.stop_clickhouse()
    node7.copy_file_to_container(
        os.path.join(CONFIG_DIR, "enable_keeper7.xml"),
        "/etc/clickhouse-server/config.d/enable_keeper7.xml",
    )

    p = Pool(3)
    waiter1 = p.apply_async(start, (node6,))
    waiter2 = p.apply_async(start, (node7,))

    command = {
        "max_action_wait_time_ms": 30000,
        "max_total_wait_time_ms": 600000,
        "preconditions": {
              "leaders": [4, 5],
              "members": [3, 4, 5]
        },
        "actions": [
            {
                "set_priority": [{"id": 3, "priority": 1}]
            },
            {
                "add_members": [
                    {
                        "id": 6,
                        "endpoint": "node6:9234",
                        "priority": 100,
                    },
                    {
                        "id": 7,
                        "endpoint": "node7:9234",
                        "priority": 100,
                    },
                ],
                "retry": 3
            },
            {
                "transfer_leadership": [6, 7]
            },
            {
                "set_priority": [{"id": 4, "priority": 0}, {"id": 5, "priority": 0}]
            }
        ]
    }
    json_command = json.dumps(command)
    print(json_command)
    print(zk.get("/keeper/config"))

    result_str = keeper_utils.send_4lw_cmd(started_cluster, node5, cmd="rcfg", port=9181, argument=json_command, timeout_sec=180)
    print("Result:", result_str)
    result = json.loads(result_str)
    assert result["status"] == "ok"
    waiter1.wait()
    waiter2.wait()

    leader = keeper_utils.get_leader(started_cluster, [node3, node4, node5, node6, node7])
    assert leader.name in ["node6", "node7"]

    content = zk.get("/keeper/config")[0].decode("utf-8")
    assert 'server.1' not in content
    assert 'server.2' not in content
    assert 'server.3=node3:9234;participant;1' in content
    assert 'server.4=node4:9234;participant;0' in content
    assert 'server.5=node5:9234;participant;0' in content
    assert 'server.6=node6:9234;participant;100' in content
    assert 'server.7=node7:9234;participant;100' in content


def test_reconfig_option1(started_cluster):
    zk = keeper_utils.get_fake_zk(cluster, "node3", timeout=30)
    zk.create("/test_reconfig_option1", b"data1")
    content = zk.get("/keeper/config")[0].decode("utf-8")
    if 'server.1' in content:
        five_to_three_reconfig(started_cluster)
    else:
        three_to_five_reconfig(started_cluster)

    assert zk.get("/test_reconfig_option1")[0] == b"data1"

def test_reconfig_option2(started_cluster):
    zk = keeper_utils.get_fake_zk(cluster, "node3", timeout=30)
    zk.create("/test_reconfig_option2", b"data2")
    content = zk.get("/keeper/config")[0].decode("utf-8")
    if 'server.1' in content:
        five_to_three_reconfig(started_cluster)
    else:
        three_to_five_reconfig(started_cluster)

    assert zk.get("/test_reconfig_option2")[0] == b"data2"

def test_no_remove_itself(started_cluster):
    command = {
        "actions": [
            {
                "remove_members": [3]
            },
        ]
    }
    json_command = json.dumps(command)
    result_str = keeper_utils.send_4lw_cmd(started_cluster, node3, cmd="rcfg", port=9181, argument=json_command, timeout_sec=300)
    result = json.loads(result_str)
    assert result["status"] == "error"
    assert "Reconfigure command cannot remove current server id" in result["message"]

def test_precondition_failure(started_cluster):
    command = {
        "preconditions": {
           "leaders": [3],
        },
    }
    json_command = json.dumps(command)
    result_str = keeper_utils.send_4lw_cmd(started_cluster, node3, cmd="rcfg", port=9181, argument=json_command, timeout_sec=300)
    result = json.loads(result_str)
    assert result["status"] == "error"
    assert "expected leader id" in result["message"]

    command = {
        "preconditions": {
           "members": [5,6,7,8,9],
        },
    }
    json_command = json.dumps(command)
    result_str = keeper_utils.send_4lw_cmd(started_cluster, node3, cmd="rcfg", port=9181, argument=json_command, timeout_sec=300)
    result = json.loads(result_str)
    assert result["status"] == "error"
    assert "found in cluster, but precondition" in result["message"]
