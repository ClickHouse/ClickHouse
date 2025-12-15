import json
import logging
import os
import sys

import pytest

from helpers.cluster import ClickHouseCluster

from .http_auth_server import GOOD_PASSWORD, TEST_CASES

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def run_echo_server():
    tmp = instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "http_auth_server.py"),
        "/http_auth_server.py",
    )

    tmp = instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /http_auth_server.py > /var/log/clickhouse-server/http_auth_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    for _ in range(0, 10):
        ping_response = instance.exec_in_container(
            ["curl", "-s", f"http://localhost:8000/health"],
            nothrow=True,
        )
        logging.debug(f"Reply: {ping_response}")
        if ping_response == "OK":
            return

    raise Exception("Echo server is not responding")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_echo_server()
        yield cluster
    finally:
        cluster.shutdown()


def test_user_from_config_basic_auth_pass(started_cluster):
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


def test_user_create_basic_auth_pass(started_cluster):
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


def test_basic_auth_failed(started_cluster):
    assert "good_user: Authentication failed" in instance.query_and_get_error(
        "SELECT currentUser()", user="good_user", password="bad_password"
    )

def test_header_failed(started_cluster):
    for header_name in ["Custom-Header", "CUSTOM-HEADER", "custom-header"]:
        ping_response = instance.exec_in_container(
            ["curl", "-s", "-u", "good_user:bad_password", "-H", f"{header_name}: ok",  "--data", f"SELECT 2+2", f"http://localhost:8123"],
            nothrow=True,
        )
        assert ping_response == "4\n"


def test_session_settings_from_auth_response(started_cluster):
    for user, case in TEST_CASES.items():
        query_id = f"test_query_{user}"
        password = "good_password"

        assert (
            instance.query(
                "SELECT currentUser()",
                user=user,
                password=password,
                query_id=query_id,
            )
            == f"{user}\n"
        )
        instance.query("SYSTEM FLUSH LOGS")

        if isinstance(case, dict):
            # Check getSetting()
            for key, value in case.get("get_settings", {}).items():
                assert (
                    instance.query(
                        f"SELECT getSetting('{key}')", user=user, password=password
                    ).strip()
                    == value
                )

            # Check system.settings
            for key, value in case.get("dump_settings", {}).items():
                assert (
                    instance.query(
                        f"SELECT name, value, description from system.settings where name = '{key}' FORMAT TSVRaw",
                        user=user,
                        password=password,
                    ).strip()
                    == f"{key}\t{value}\tCustom"
                )

            # Check system.query_log
            res = instance.query(
                f"select Settings from system.query_log where type = 'QueryFinish' and query_id = '{query_id}' FORMAT JSON"
            )
            res = json.loads(res)
            query_settings = res["data"][0]["Settings"]

            for key, value in case.get("dump_settings", {}).items():
                assert query_settings.get(key) == value
