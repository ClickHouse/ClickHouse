import json
import os
import shlex
import typing
from datetime import datetime, timedelta, timezone

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

from .http_auth_server import GOOD_PASSWORD, USER_RESPONSES

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml", "configs/cluster.xml"],
    user_configs=["configs/users.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml", "configs/cluster.xml"],
    user_configs=["configs/users.xml"],
)
instance = node1
nodes = [node1, node2]
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def run_echo_server(node):
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "http_auth_server.py"),
        "/http_auth_server.py",
    )

    node.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /http_auth_server.py > /var/log/clickhouse-server/http_auth_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )

    def check_server() -> str:
        return node.exec_in_container(
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
        for node in nodes:
            run_echo_server(node)
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
    for user in ["test_user_1", "test_user_2", "test_user_3", "test_user_4"]:
        response = USER_RESPONSES[user]
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


def create_http_user(node, user, valid_until=None):
    node.query(f"DROP USER IF EXISTS {user}")
    query = (
        f"CREATE USER {user} IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'BASIC'"
    )
    if valid_until is not None:
        query += f" VALID UNTIL '{valid_until}'"
    node.query(query)


def query_as(user, query):
    return node1.query(query, user=user, password=GOOD_PASSWORD)


def query_error_as(user, query):
    return node1.query_and_get_error(query, user=user, password=GOOD_PASSWORD)


def ensure_http_reader_objects():
    node1.query("CREATE TABLE IF NOT EXISTS http_auth_role_test (x UInt8) ENGINE=Memory")
    node1.query("CREATE ROLE IF NOT EXISTS http_reader")
    node1.query("GRANT SELECT ON http_auth_role_test TO http_reader")


def test_roles_grant_session_access_and_are_not_persisted(started_cluster):
    node1.query("DROP TABLE IF EXISTS http_auth_role_test")
    node1.query("DROP ROLE IF EXISTS http_reader")
    node1.query("CREATE TABLE http_auth_role_test (x UInt8) ENGINE=Memory")
    node1.query("INSERT INTO http_auth_role_test VALUES (42)")
    node1.query("CREATE ROLE http_reader")
    node1.query("GRANT SELECT ON http_auth_role_test TO http_reader")
    create_http_user(node1, "role_user")

    assert query_as("role_user", "SELECT x FROM http_auth_role_test") == "42\n"
    assert "http_reader" not in node1.query("SHOW GRANTS FOR role_user")


def test_multiple_roles(started_cluster):
    ensure_http_reader_objects()
    node1.query("DROP TABLE IF EXISTS role_a_test")
    node1.query("DROP TABLE IF EXISTS role_b_test")
    node1.query("DROP ROLE IF EXISTS role_a")
    node1.query("DROP ROLE IF EXISTS role_b")
    node1.query("CREATE TABLE role_a_test (x UInt8) ENGINE=Memory")
    node1.query("CREATE TABLE role_b_test (x UInt8) ENGINE=Memory")
    node1.query("CREATE ROLE role_a")
    node1.query("CREATE ROLE role_b")
    node1.query("GRANT SELECT ON role_a_test TO role_a")
    node1.query("GRANT SELECT ON role_b_test TO role_b")
    create_http_user(node1, "multi_role_user")

    assert query_as("multi_role_user", "SELECT count() FROM role_a_test") == "0\n"
    assert query_as("multi_role_user", "SELECT count() FROM role_b_test") == "0\n"


def test_unknown_roles_reject_authentication(started_cluster):
    ensure_http_reader_objects()
    create_http_user(node1, "unknown_role_user")
    assert "Some roles returned by the HTTP authentication server are unknown" in query_error_as(
        "unknown_role_user", "SELECT count() FROM http_auth_role_test"
    )
    assert (
        node1.query(
            "SELECT count() FROM system.roles WHERE name = 'role_that_does_not_exist'"
        )
        == "0\n"
    )


@pytest.mark.parametrize(
    "user",
    [
        "malformed_roles_type_user",
        "malformed_roles_number_user",
        "malformed_roles_bool_user",
    ],
)
def test_malformed_roles_reject_authentication(started_cluster, user):
    create_http_user(node1, user)
    assert "Authentication failed" in query_error_as(user, "SELECT 1")


def test_partial_settings_and_roles_are_independent(started_cluster):
    ensure_http_reader_objects()
    create_http_user(node1, "partial_settings_user")
    assert query_as("partial_settings_user", "SELECT getSetting('auth_a_valid')") == "15\n"
    query_as("partial_settings_user", "SELECT count() FROM http_auth_role_test")

    create_http_user(node1, "malformed_settings_user")
    query_as("malformed_settings_user", "SELECT count() FROM http_auth_role_test")
    assert node1.contains_in_log(
        "Failed to parse settings from authentication response. Skip them."
    )


def test_http_interface_receives_roles_and_named_session_refreshes_them(started_cluster):
    node1.query("DROP TABLE IF EXISTS named_session_test")
    node1.query("DROP ROLE IF EXISTS named_session_reader")
    node1.query("CREATE TABLE named_session_test (x UInt8) ENGINE=Memory")
    node1.query("INSERT INTO named_session_test VALUES (7)")
    node1.query("CREATE ROLE named_session_reader")
    node1.query("GRANT SELECT ON named_session_test TO named_session_reader")
    create_http_user(node1, "named_session_user")

    url = "http://localhost:8123/?session_id=external-roles-refresh"
    first = node1.exec_in_container(
        [
            "curl",
            "-sS",
            "-u",
            f"named_session_user:{GOOD_PASSWORD}",
            "-H",
            "Custom-Header: roles-reader",
            "--data-binary",
            "SELECT x FROM named_session_test",
            url,
        ]
    )
    assert first == "7\n"

    second = node1.exec_in_container(
        [
            "curl",
            "-sS",
            "-u",
            f"named_session_user:{GOOD_PASSWORD}",
            "-H",
            "Custom-Header: roles-none",
            "--data-binary",
            "SELECT x FROM named_session_test",
            url,
        ],
        nothrow=True,
    )
    assert "Not enough privileges" in second


def test_external_role_settings_profile_applies_to_fresh_session(started_cluster):
    node1.query("DROP USER IF EXISTS external_role_settings_user")
    node1.query("DROP SETTINGS PROFILE IF EXISTS external_role_with_profile_settings")
    node1.query("DROP ROLE IF EXISTS external_role_with_profile")
    node1.query("CREATE ROLE external_role_with_profile")
    node1.query(
        "CREATE SETTINGS PROFILE external_role_with_profile_settings "
        "SETTINGS max_threads = 1 TO external_role_with_profile"
    )
    create_http_user(node1, "external_role_settings_user")

    assert (
        query_as("external_role_settings_user", "SELECT getSetting('max_threads')")
        == "1\n"
    )


def test_named_session_reauthentication_refreshes_role_derived_session_limit(started_cluster):
    node1.query("DROP USER IF EXISTS named_session_limits_user")
    node1.query("DROP SETTINGS PROFILE IF EXISTS named_session_unlimited_settings")
    node1.query("DROP SETTINGS PROFILE IF EXISTS named_session_limited_settings")
    node1.query("DROP ROLE IF EXISTS named_session_unlimited")
    node1.query("DROP ROLE IF EXISTS named_session_limited")
    node1.query("CREATE ROLE named_session_unlimited")
    node1.query("CREATE ROLE named_session_limited")
    node1.query(
        "CREATE SETTINGS PROFILE named_session_unlimited_settings "
        "SETTINGS max_sessions_for_user = 0 TO named_session_unlimited"
    )
    node1.query(
        "CREATE SETTINGS PROFILE named_session_limited_settings "
        "SETTINGS max_sessions_for_user = 1 TO named_session_limited"
    )
    create_http_user(node1, "named_session_limits_user")

    def query_named_session(
        session_id, role_header, query, detach=False, nothrow=False
    ):
        return node1.exec_in_container(
            [
                "curl",
                "-sS",
                "-u",
                f"named_session_limits_user:{GOOD_PASSWORD}",
                "-H",
                f"Custom-Header: {role_header}",
                "--data-binary",
                query,
                f"http://localhost:8123/?session_id={session_id}",
            ],
            detach=detach,
            nothrow=nothrow,
        )

    assert (
        query_named_session("external-role-limit-a", "roles-unlimited", "SELECT 1")
        == "1\n"
    )

    query_named_session(
        "external-role-limit-b",
        "roles-unlimited",
        "SELECT sleep(10) SETTINGS function_sleep_max_microseconds_per_block = 10000000",
        detach=True,
    )
    wait_condition(
        lambda: node1.query(
            "SELECT count() FROM system.processes "
            "WHERE user = 'named_session_limits_user'"
        ),
        lambda response: response == "1\n",
        max_attempts=20,
        delay=0.5,
    )

    error = query_named_session(
        "external-role-limit-a", "roles-limited", "SELECT 1", nothrow=True
    )
    assert "has overflown session count 1" in error


def test_expired_and_zero_deadlines_reject_authentication(started_cluster):
    for user in ["expiry_past_user", "expiry_zero_user"]:
        create_http_user(node1, user)
        assert "Authentication failed" in query_error_as(user, "SELECT 1")


@pytest.mark.parametrize(
    "user",
    [
        "expiry_string_user",
        "expiry_bool_user",
        "expiry_fraction_user",
        "expiry_out_of_range_user",
    ],
)
def test_malformed_deadlines_reject_authentication(started_cluster, user):
    create_http_user(node1, user)
    assert "Authentication failed" in query_error_as(user, "SELECT 1")


def run_expiring_native_session(user):
    command = (
        "/usr/bin/clickhouse client --user "
        + shlex.quote(user)
        + " --password "
        + shlex.quote(GOOD_PASSWORD)
        + " --multiquery --query "
        + shlex.quote(
            "SELECT 1; "
            "SELECT sleep(17) SETTINGS function_sleep_max_microseconds_per_block = 17000000; "
            "SELECT 2;"
        )
        + " 2>&1"
    )
    return node1.exec_in_container(["bash", "-c", command], nothrow=True)


def test_helper_deadline_expires_a_persistent_native_session(started_cluster):
    create_http_user(node1, "expiry_future_user", "2099-01-01 00:00:00")
    output = run_expiring_native_session("expiry_future_user")
    assert output.startswith("1\n0\n")
    assert "Authentication method used has expired" in output


def test_local_deadline_cannot_be_extended_by_helper(started_cluster):
    local_deadline = datetime.now(timezone.utc) + timedelta(seconds=15)
    create_http_user(
        node1,
        "expiry_later_user",
        local_deadline.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )
    output = run_expiring_native_session("expiry_later_user")
    assert output.startswith("1\n0\n")
    assert "Authentication method used has expired" in output


def test_external_roles_are_forwarded_with_other_current_roles(started_cluster):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS interserver_helper_roles_test")
        node.query("DROP TABLE IF EXISTS interserver_initiator_roles_test")
        node.query("DROP ROLE IF EXISTS helper_reader")
        node.query("DROP ROLE IF EXISTS initiator_reader")
        node.query("CREATE TABLE interserver_helper_roles_test (x UInt8) ENGINE=Memory")
        node.query("CREATE TABLE interserver_initiator_roles_test (y UInt8) ENGINE=Memory")
        node.query("INSERT INTO interserver_helper_roles_test VALUES (1)")
        node.query("INSERT INTO interserver_initiator_roles_test VALUES (2)")
        node.query("CREATE ROLE helper_reader")
        node.query("CREATE ROLE initiator_reader")
        node.query("GRANT SELECT ON interserver_helper_roles_test TO helper_reader")
        node.query("GRANT SELECT ON interserver_initiator_roles_test TO initiator_reader")
        create_http_user(node, "interserver_user")

    node1.query("GRANT initiator_reader TO interserver_user")
    node1.query("ALTER USER interserver_user DEFAULT ROLE initiator_reader")
    node1.query("GRANT READ ON REMOTE TO interserver_user")
    assert query_as(
        "interserver_user",
        "SELECT sum(x) FROM clusterAllReplicas("
        "'external_roles_cluster', default.interserver_helper_roles_test) "
        "UNION ALL SELECT sum(y) FROM clusterAllReplicas("
        "'external_roles_cluster', default.interserver_initiator_roles_test) "
        "SETTINGS push_external_roles_in_interserver_queries=1",
    ) == "2\n4\n"


def test_unknown_forwarded_role_fails_closed(started_cluster):
    node1.query("DROP ROLE IF EXISTS initiator_only_role")
    node2.query("DROP ROLE IF EXISTS initiator_only_role")
    node1.query("CREATE ROLE initiator_only_role")
    node1.query("GRANT SELECT ON system.one TO initiator_only_role")
    for node in nodes:
        create_http_user(node, "interserver_unknown_user")
    node1.query("GRANT READ ON REMOTE TO interserver_unknown_user")

    error = query_error_as(
        "interserver_unknown_user",
        "SELECT count() FROM clusterAllReplicas("
        "'external_roles_cluster', system.one) "
        "SETTINGS push_external_roles_in_interserver_queries=1",
    )
    assert "Not all of the initiator's current roles are known on this node" in error
