import json
import os
import time
import typing

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

from .http_auth_server import GOOD_PASSWORD, USER_RESPONSES

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)
dir_nodes = [
    cluster.add_instance(
        name,
        main_configs=["configs/http_directory.xml"],
        user_configs=["configs/users.xml"],
        with_zookeeper=False,
    )
    for name in ["node_dir1", "node_dir2"]
]
node_dir1, node_dir2 = dir_nodes
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def run_echo_server():
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "http_auth_server.py"),
        "/http_auth_server.py",
    )

    instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /http_auth_server.py > /var/log/clickhouse-server/http_auth_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_server() -> str:
        return instance.exec_in_container(
            ["curl", "-s", "http://localhost:8000/health"],
            nothrow=True,
        )

    wait_condition(
        check_server,
        lambda response: response == "OK",
        max_attempts=20,
        delay=0.5,
    )


@pytest.fixture(scope="module")
def started_cluster() -> typing.Generator[ClickHouseCluster, None, None]:
    try:
        cluster.start()
        run_echo_server()
        for n in dir_nodes:
            n.query(
                "CREATE ROLE r_reader, r_a, r_b, r_admin;"
                "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;"
                "INSERT INTO t VALUES (1), (2), (3);"
                "GRANT SELECT ON t TO r_reader;"
                "GRANT REMOTE ON *.* TO r_reader"
            )
        yield cluster
    finally:
        cluster.shutdown()


def test_user_from_config_basic_auth_pass(started_cluster: ClickHouseCluster):
    assert (
        instance.query("SHOW CREATE USER good_user")
        == "CREATE USER good_user IDENTIFIED WITH http SERVER \\'basic_server\\' SCHEME \\'BASIC\\' SETTINGS PROFILE `default`\n"
    )
    assert (
        instance.query(
            "SELECT currentUser()", user="good_user", password="good_password"
        )
        == "good_user\n"
    )


def test_user_create_basic_auth_pass(started_cluster: ClickHouseCluster):
    instance.query(
        "CREATE USER basic_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'BASIC'"
    )

    assert (
        instance.query("SHOW CREATE USER basic_user")
        == "CREATE USER basic_user IDENTIFIED WITH http SERVER \\'basic_server\\' SCHEME \\'BASIC\\'\n"
    )
    assert (
        instance.query(
            "SELECT currentUser()", user="basic_user", password=GOOD_PASSWORD
        )
        == "basic_user\n"
    )

    instance.query("DROP USER basic_user")


def test_basic_auth_failed(started_cluster: ClickHouseCluster):
    assert "good_user: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="good_user", password="bad_password"
    )


def test_header_failed(started_cluster: ClickHouseCluster):
    for header_name in ["Custom-Header", "CUSTOM-HEADER", "custom-header"]:
        ping_response = instance.exec_in_container(
            [
                "curl",
                "-s",
                "-u",
                "good_user:bad_password",
                "-H",
                f"{header_name}: ok",
                "--data",
                "SELECT 2+2",
                "http://localhost:8123",
            ],
            nothrow=True,
        )
        assert ping_response == "4\n"


def test_session_settings_from_auth_response(started_cluster: ClickHouseCluster):
    for user, response in USER_RESPONSES.items():
        query_id = f"test_query_{user}"
        assert (
            instance.query(
                "SELECT currentUser()",
                user=user,
                password="good_password",
                query_id=query_id,
            )
            == f"{user}\n"
        )
        instance.query("SYSTEM FLUSH LOGS")

        res = instance.query(
            f"select Settings from system.query_log where type = 'QueryFinish' and query_id = '{query_id}' FORMAT JSON"
        )

        res = json.loads(res)
        query_settings = res["data"][0]["Settings"]

        if isinstance(response, dict):
            for key, value in response.get("settings", {}).items():
                assert query_settings.get(key) == value


# ---- `http` user directory ----


def directory_count(user: str) -> int:
    return int(
        instance.exec_in_container(
            ["curl", "-s", f"http://localhost:8000/count?user={user}"]
        )
    )


def http_as(node, user: str, query: str, tenant=None, session_id=None, password=GOOD_PASSWORD):
    params = {"query": query}
    if session_id:
        params["session_id"] = session_id
    headers = {"X-Tenant": tenant} if tenant else {}
    return node.http_request("", method="POST", params=params, headers=headers, auth=(user, password))


def test_directory_login_roles_settings(started_cluster: ClickHouseCluster):
    node = node_dir1
    assert node.query("SELECT currentUser(), currentRoles()", user="alice", password=GOOD_PASSWORD) == "alice\t['r_reader']\n"
    assert node.query("SELECT getSetting('max_threads')", user="alice", password=GOOD_PASSWORD) == "3\n"
    assert node.query("SELECT count() FROM t", user="alice", password=GOOD_PASSWORD) == "3\n"
    # Roles are enabled but not granted.
    assert node.query("SHOW GRANTS", user="alice", password=GOOD_PASSWORD) == ""
    assert "r_reader" in node.query_and_get_error("SET ROLE r_reader", user="alice", password=GOOD_PASSWORD)
    assert node.query("SET ROLE NONE; SELECT currentRoles()", user="alice", password=GOOD_PASSWORD) == "['r_reader']\n"

    assert node.query("SELECT currentUser(), currentRoles()", user="plain", password=GOOD_PASSWORD) == "plain\t[]\n"
    assert "Not enough privileges" in node.query_and_get_error("SELECT count() FROM t", user="plain", password=GOOD_PASSWORD)


def test_directory_role_policy(started_cluster: ClickHouseCluster):
    for user in ["bad_role", "ghost_role"]:
        assert "Authentication failed" in node_dir1.query_and_get_error("SELECT 1", user=user, password=GOOD_PASSWORD)


def test_directory_fall_through(started_cluster: ClickHouseCluster):
    node = node_dir1
    node.query("CREATE USER local_u IDENTIFIED BY 'pw'")
    try:
        assert node.query("SELECT currentUser()", user="local_u", password="pw") == "local_u\n"
        assert "Authentication failed" in node.query_and_get_error("SELECT 1", user="local_u", password="wrong")
        # A name known to another directory is never sent to the service.
        assert directory_count("local_u") == 0
    finally:
        node.query("DROP USER local_u")

    assert "Authentication failed" in node.query_and_get_error("SELECT 1", user="broken", password=GOOD_PASSWORD)
    assert "Authentication failed" in node.query_and_get_error("SELECT 1", user="nobody_here", password=GOOD_PASSWORD)
    assert "Authentication failed" in node.query_and_get_error("SELECT 1", user="alice", password="wrong_password")


def test_directory_per_authentication_roles(started_cluster: ClickHouseCluster):
    for tenant, roles in [("a", "['r_a']"), ("b", "['r_b']"), ("a", "['r_a']")]:
        assert http_as(node_dir1, "tenant_user", "SELECT currentRoles()", tenant=tenant).text == roles + "\n"


def test_directory_authentication_cache(started_cluster: ClickHouseCluster):
    node = node_dir1
    user = "cached_user"

    def login(password=GOOD_PASSWORD):
        return node.query_and_get_error("SELECT 1", user=user, password=password)

    # The TTL is short, so tolerate an expiry between two logins.
    for _ in range(5):
        before = directory_count(user)
        node.query("SELECT 1", user=user, password=GOOD_PASSWORD)
        node.query("SELECT 1", user=user, password=GOOD_PASSWORD)
        if directory_count(user) - before == 1:
            break
    else:
        assert False, "the authentication result was not cached"

    # Failures are not cached and reach the service.
    before = directory_count(user)
    assert "Authentication failed" in login("wrong_password")
    assert directory_count(user) == before + 1

    # After the TTL a new login reaches the service again.
    before = directory_count(user)
    deadline = time.monotonic() + 30
    while directory_count(user) == before:
        assert time.monotonic() < deadline, "the cache entry never expired"
        node.query("SELECT 1", user=user, password=GOOD_PASSWORD)
        time.sleep(0.2)


def test_directory_named_session(started_cluster: ClickHouseCluster):
    node = node_dir1
    assert http_as(node, "tenant_user", "SELECT currentRoles()", tenant="a", session_id="s1").text == "['r_a']\n"
    assert http_as(node, "tenant_user", "SELECT 1", tenant="b", session_id="s1").status_code != 200
    assert http_as(node, "tenant_user", "SELECT currentRoles()", tenant="a", session_id="s1").text == "['r_a']\n"


def test_directory_distributed_select(started_cluster: ClickHouseCluster):
    user = "dist_user"
    assert (
        node_dir1.query(
            "SELECT count() FROM cluster('dir_cluster', default, t)", user=user, password=GOOD_PASSWORD
        )
        == "6\n"
    )
    # The shard materialized the user from the interserver credentials and did not call the service.
    assert directory_count(user) == 1
    assert node_dir2.query(f"SELECT count() FROM system.users WHERE name = '{user}'") == "1\n"
