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
    assert "cwd" not in response.json()

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
    assert "cwd" not in body  # unchanged on failure
    assert "does not exist" in body["result"]

    with keeper_utils.KeeperClient.from_cluster(
        cluster, keeper_ip=leader.ip_address, port=9181
    ) as client:
        client.rmr(dirname)


def test_http_commands_rejects_invalid_cwd(started_cluster):
    leader: ClickHouseInstance = keeper_utils.get_leader(cluster, [node1, node2, node3])

    empty_cwd_response = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"command": "ls", "cwd": ""},
    )
    assert empty_cwd_response.status_code == 200

    for invalid_cwd in ("..", "relative/path"):
        response = requests.get(
            "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
            params={"command": "ls", "cwd": invalid_cwd},
        )
        assert response.status_code == 400
        assert "Invalid cwd" in response.text

    complete_response = requests.get(
        "http://{host}:{port}/api/v1/commands".format(host=leader.ip_address, port=9182),
        params={"complete": "ls", "cwd": ".."},
    )
    assert complete_response.status_code == 400
    assert "Invalid cwd" in complete_response.text

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

    # Leading whitespace must not break command-name completion: filter against the
    # trimmed command prefix and keep indentation via replace_start.
    indented_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": "  cre", "cwd": "/"},
    )
    assert indented_resp.status_code == 200
    indented_body = indented_resp.json()
    assert indented_body["replace_start"] == 2
    assert "create" in indented_body["completions"]
    assert all(c.startswith("cre") for c in indented_body["completions"])
    assert "  cre"[: indented_body["replace_start"]] + "create" == "  create"

    spaces_only_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": "  ", "cwd": "/"},
    )
    assert spaces_only_resp.status_code == 200
    spaces_only_body = spaces_only_resp.json()
    assert spaces_only_body["replace_start"] == 2
    assert "ls" in spaces_only_body["completions"]
    assert "create" in spaces_only_body["completions"]

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
    assert multi_body["replace_start"] == len("create ".encode("utf-8"))
    multi_matches = [c for c in multi_body["completions"] if child_a in c]
    assert len(multi_matches) == 1
    # replace_start is a UTF-8 byte offset into `complete`, not a Unicode index.
    multi_head = multi_prefix.encode("utf-8")[: multi_body["replace_start"]].decode("utf-8")
    rewritten = multi_head + multi_matches[0] + multi_suffix
    assert rewritten.endswith(" myvalue")
    assert f"/{dirname}/{child_a}" in rewritten

    # Leading whitespace before the command must still allow path completion.
    indented_path_prefix = f"  create /{dirname}/{prefix}_a"
    indented_path_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": indented_path_prefix, "cwd": "/"},
    )
    assert indented_path_resp.status_code == 200
    indented_path_body = indented_path_resp.json()
    assert indented_path_body["replace_start"] == len("  create ".encode("utf-8"))
    indented_path_matches = [
        c for c in indented_path_body["completions"] if child_a in c
    ]
    assert len(indented_path_matches) == 1
    indented_rewritten = (
        indented_path_prefix.encode("utf-8")[: indented_path_body["replace_start"]].decode(
            "utf-8"
        )
        + indented_path_matches[0]
        + multi_suffix
    )
    assert indented_rewritten.startswith("  create ")
    assert f"/{dirname}/{child_a}" in indented_rewritten

    # Non-ASCII text before replace_start must keep the UTF-8 byte offset contract.
    # Python/JS string indexes would land mid-argument if treated as code points.
    unicode_head = "create '已有' "
    unicode_prefix = f"{unicode_head}/{dirname}/{prefix}_a"
    unicode_resp = requests.get(
        "http://{host}:{port}/api/v1/commands".format(
            host=leader.ip_address, port=9182
        ),
        params={"complete": unicode_prefix, "cwd": "/"},
    )
    assert unicode_resp.status_code == 200
    unicode_body = unicode_resp.json()
    assert unicode_body["replace_start"] == len(unicode_head.encode("utf-8"))
    assert unicode_body["replace_start"] != len(unicode_head)  # code points != bytes
    unicode_matches = [c for c in unicode_body["completions"] if child_a in c]
    assert len(unicode_matches) == 1
    rewritten_unicode = (
        unicode_prefix.encode("utf-8")[: unicode_body["replace_start"]].decode("utf-8")
        + unicode_matches[0]
        + multi_suffix
    )
    assert rewritten_unicode.startswith(unicode_head)
    assert f"/{dirname}/{child_a}" in rewritten_unicode
    assert rewritten_unicode.endswith(" myvalue")

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
