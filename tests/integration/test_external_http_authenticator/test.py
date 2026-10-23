import json
import os
import sys
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

# A native protocol client that can send the `ResetSession` packet, shared with the stateless tests.
sys.path.insert(0, os.path.join(SCRIPT_DIR, "../../queries/0_stateless/helpers"))
from native_session_client import NativeSessionClient, expect_error


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


def test_reset_session_restores_settings_from_auth_response(started_cluster: ClickHouseCluster):
    # The `ResetSession` packet makes the session again as at login, so the settings that the
    # authentication server returned come back after the session changed them.
    with NativeSessionClient(
        user="test_user_1",
        password=GOOD_PASSWORD,
        database="default",
        host=instance.ip_address,
        port=9000,
    ) as client:
        assert client.value("SELECT toString(getSetting('auth_num'))") == "15"

        client.query("SET auth_num = 99")
        assert client.value("SELECT toString(getSetting('auth_num'))") == "99"

        client.reset_session()
        assert client.value("SELECT toString(getSetting('auth_num'))") == "15"


def test_reset_session_replays_profile_and_preserves_state_on_failure(started_cluster):
    profile = "CREATE SETTINGS PROFILE reset_auth_profile SETTINGS max_block_size = 4242, max_threads = 2 MAX 4"
    instance.query(profile)
    instance.query(
        "CREATE USER reset_profile_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'BASIC'"
    )
    instance.query("GRANT CREATE TEMPORARY TABLE ON *.* TO reset_profile_user")
    instance.query("GRANT TABLE ENGINE ON Memory TO reset_profile_user")
    try:
        with NativeSessionClient(
            user="reset_profile_user",
            password=GOOD_PASSWORD,
            database="default",
            host=instance.ip_address,
            port=9000,
        ) as client:
            client.query("SET max_block_size = 1111, auth_num = 99")
            client.reset_session()
            assert client.query(
                "SELECT toString(getSetting('max_block_size')), toString(getSetting('auth_num'))"
            ) == [["4242", "15"]]
            expect_error(
                "SETTING_CONSTRAINT_VIOLATION", client.query, "SET max_threads = 8"
            )

            client.query("SET max_block_size = 2222, param_kept = 'kept'")
            client.query("CREATE TEMPORARY TABLE reset_auth_kept (x String)")
            client.query("INSERT INTO reset_auth_kept VALUES ('kept')")
            instance.query("DROP SETTINGS PROFILE reset_auth_profile")
            expect_error("THERE_IS_NO_PROFILE", client.reset_session)
            assert client.query(
                "SELECT toString(getSetting('max_block_size')), {kept:String}"
            ) == [["2222", "kept"]]
            assert client.value("SELECT x FROM reset_auth_kept") == "kept"

            instance.query(profile)
            client.reset_session()
            assert (
                client.value("SELECT toString(getSetting('max_block_size'))") == "4242"
            )
            expect_error(
                "UNKNOWN_QUERY_PARAMETER", client.query, "SELECT {kept:String}"
            )
            expect_error("UNKNOWN_TABLE", client.query, "SELECT x FROM reset_auth_kept")
    finally:
        instance.query("DROP USER IF EXISTS reset_profile_user")
        instance.query("DROP SETTINGS PROFILE IF EXISTS reset_auth_profile")
