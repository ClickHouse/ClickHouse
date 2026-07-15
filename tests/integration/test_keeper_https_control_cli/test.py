#!/usr/bin/env python3

import pytest
import requests
import urllib3

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
SSL_CONFIGS = [
    "configs/ssl_conf.xml",
    "configs/dhparam.pem",
    "configs/server.crt",
    "configs/server.key",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"] + SSL_CONFIGS,
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"] + SSL_CONFIGS,
    stay_alive=True,
    with_remote_database_disk=False,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"] + SSL_CONFIGS,
    stay_alive=True,
    with_remote_database_disk=False,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_https_command_response(node):
    session = requests.Session()
    session.trust_env = False
    return session.get(
        "https://{host}:{port}/api/v1/commands?command=conf".format(
            host=node.ip_address, port=9183
        ),
        verify=False,
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start(connection_timeout=450.0)
        yield cluster
    finally:
        cluster.shutdown()


def test_https_commands_basic_responses(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    response = get_https_command_response(leader)
    assert response.status_code == 200

    command_data = response.json()
    assert command_data["result"] == keeper_utils.send_4lw_cmd(cluster, leader, "conf")

    follower = keeper_utils.get_any_follower(cluster, [node1, node2, node3])
    response = get_https_command_response(follower)
    assert response.status_code == 200

    command_data = response.json()
    assert command_data["result"] == keeper_utils.send_4lw_cmd(cluster, follower, "conf")
