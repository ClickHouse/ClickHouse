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
        cluster.start(connection_timeout=450.0)
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


def test_http_commands_no_host_path_leak_via_proc_cwd(started_cluster):
    leader: ClickHouseInstance = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": "cd foo", "cwd": "/proc/self/cwd"},
    )
    assert response.status_code == 200
    assert "/proc/self/cwd/foo" in response.json()["result"]
    assert "does not exist" in response.json()["result"]


def test_http_commands_rejects_invalid_cwd(started_cluster):
    leader: ClickHouseInstance = keeper_utils.get_leader(cluster, [node1, node2, node3])

    empty_cwd_response = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": "cd foo", "cwd": ""},
    )
    assert empty_cwd_response.status_code == 200
    # `cd` prints the resolved znode in quotes.
    assert '"/foo"' in empty_cwd_response.json()["result"]

    for invalid_cwd in ("..", "relative/path"):
        response = requests.get(
            "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
            params={"command": "ls", "cwd": invalid_cwd},
        )
        assert response.status_code == 400
        assert response.json()["message"] == "Invalid cwd: must be an absolute path starting with '/'."


def test_http_commands_rejects_invalid_cwd_without_quorum(started_cluster):
    url = "http://{host}:{port}/api/v1/commands".format(host=node1.ip_address, port=9182)
    # After the restart node1 has no Keeper session, and without quorum it cannot create one.
    node1.restart_clickhouse()
    node2.stop_clickhouse()
    node3.stop_clickhouse()
    try:
        keeper_utils.wait_until_quorum_lost(cluster, node1)
        response = requests.get(url, params={"command": "ls", "cwd": "relative/path"}, timeout=60)
        assert response.status_code == 400
        assert response.json()["message"] == "Invalid cwd: must be an absolute path starting with '/'."
    finally:
        node2.start_clickhouse()
        node3.start_clickhouse()
        keeper_utils.wait_nodes(cluster, [node1, node2, node3])
