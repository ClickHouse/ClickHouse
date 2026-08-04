#!/usr/bin/env python3

import os
import uuid

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
    assert response.json()["cwd"] == "/"

    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        assert client.get("foo") == "bar"
        client.rm("foo")


def test_http_commands_cd_returns_cwd(started_cluster):
    leader: ClickHouseInstance = keeper_utils.get_leader(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())
    dirname = f"{prefix}_dir"
    spaced = f"{prefix} space node"

    create_dir = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": f"create '{dirname}' _", "cwd": "/"},
    )
    assert create_dir.status_code == 200

    create_spaced = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": f"create '/{dirname}/{spaced}' _", "cwd": "/"},
    )
    assert create_spaced.status_code == 200

    cd_ok = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": f"cd '{dirname}'", "cwd": "/"},
    )
    assert cd_ok.status_code == 200
    assert cd_ok.json()["cwd"] == f"/{dirname}"
    assert cd_ok.json()["result"] == ""

    # Quoted path with a space — parsed by backend parseKeeperArg
    cd_space = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": f"cd '/{dirname}/{spaced}'", "cwd": "/"},
    )
    assert cd_space.status_code == 200
    assert cd_space.json()["cwd"] == f"/{dirname}/{spaced}"

    cd_missing = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": "cd /does_not_exist_cd_cwd", "cwd": f"/{dirname}"},
    )
    assert cd_missing.status_code == 200
    body = cd_missing.json()
    assert body["cwd"] == f"/{dirname}"  # unchanged on failure
    assert "does not exist" in body["result"]

    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        client.rmr(dirname)

def test_http_commands_complete(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())
    dirname = f"{prefix}_comp"
    child_a = f"{prefix}_alpha"
    child_b = f"{prefix}_beta"

    for path in (dirname, f"{dirname}/{child_a}", f"{dirname}/{child_b}"):
        response = requests.get(
            "http://{host}:{port}/api/v1/commands".format(
                host=leader.ip_address, port=9182
            ),
            params={"command": f"create '/{path}' _", "cwd": "/"},
        )
        assert response.status_code == 200

    # Empty complete prefix lists all registered command names.
    all_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": "", "cwd": "/"},
    )
    assert all_resp.status_code == 200
    all_commands = all_resp.json()["completions"]
    assert isinstance(all_commands, list)
    for expected in ("ls", "create", "get", "rmr", "cd", "help", "ruok", "mntr", "conf", "srvr"):
        assert expected in all_commands
    assert all_commands == sorted(all_commands)
    assert len(all_commands) == len(set(all_commands))

    # Command-name completion
    cmd_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": "cre", "cwd": "/"},
    )
    assert cmd_resp.status_code == 200
    cmd_body = cmd_resp.json()
    assert cmd_body["replace_start"] == 0
    assert "create" in cmd_body["completions"]
    assert all(c.startswith("cre") for c in cmd_body["completions"])

    # Path completion for the argument at the end of the prefix (cursor position).
    # Dashboard sends only text up to the caret, then re-appends any suffix itself.
    multi_prefix = f"create /{dirname}/{prefix}_a"
    multi_suffix = " myvalue"
    multi_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": multi_prefix, "cwd": "/"},
    )
    assert multi_resp.status_code == 200
    multi_body = multi_resp.json()
    assert multi_body["replace_start"] == len("create ")
    multi_matches = [c for c in multi_body["completions"] if child_a in c]
    assert len(multi_matches) == 1
    rewritten = (
        multi_prefix[: multi_body["replace_start"]] + multi_matches[0] + multi_suffix
    )
    assert rewritten.endswith(" myvalue")
    assert f"/{dirname}/{child_a}" in rewritten

    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        client.rmr(dirname)


def test_http_commands_quoted_semicolon_node(started_cluster):
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    prefix = str(uuid.uuid4())
    # create path with semicolon in name — must be quoted for parser
    path = f"{prefix}a;b"
    response = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": f"create '{path}' 'semival'", "cwd": "/"},
    )
    assert response.status_code == 200
    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        assert client.get(path) == "semival"
        client.rm(path)
