import json
import os
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
            ["curl", "-s", f"http://localhost:8000/health"],
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
                f"SELECT 2+2",
                f"http://localhost:8123",
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
