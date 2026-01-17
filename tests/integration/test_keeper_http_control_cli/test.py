#!/usr/bin/env python3

import os

import pytest
import requests

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

# Disable `with_remote_database_disk` as the test does not use the default Keeper.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_http_commands_basic_responses(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/api/v1/commands?command=conf".format(
            host=leader.ip_address, port=9182
        )
    )
    assert response.status_code == 200

    command_data = response.json()
    assert command_data["result"] == keeper_utils.send_4lw_cmd(cluster, leader, "conf")

    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/api/v1/commands?command=conf".format(
            host=follower.ip_address, port=9182
        )
    )
    assert response.status_code == 200

    command_data = response.json()
    assert command_data["result"] == keeper_utils.send_4lw_cmd(
        cluster, follower, "conf"
    )


def test_http_commands_cli_response(started_cluster):
    leader: ClickHouseInstance = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(
        # create 'foo' 'bar'
        "http://{host}:{port}/api/v1/commands?command=create+%27foo%27+%27bar%27".format(
            host=leader.ip_address, port=9182
        )
    )
    assert response.status_code == 200

    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        assert client.get("foo") == "bar"
        client.rm("foo")
