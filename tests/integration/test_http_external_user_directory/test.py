import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/config_main.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)
instance2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_main.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)
instance3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config_http_first.xml"],
    user_configs=["configs/users.xml"],
)
instance4 = cluster.add_instance(
    "node4",
    main_configs=["configs/config_profile_networks.xml"],
    user_configs=["configs/users.xml"],
)

GOOD_PASSWORD = "good_password"


def admin(query, **kwargs):
    return instance.query(query, user="admin_user", password="admin_password", **kwargs)


def start_mock_server(node):
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "http_auth_server.py"), "/http_auth_server.py"
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
        max_attempts=300,
        delay=0.1,
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        for node in [instance, instance2, instance3, instance4]:
            start_mock_server(node)
        # Roles used across the test file. admin_role exists but is NOT delegated.
        for role in [
            "reader",
            "analyst",
            "role_a",
            "role_b",
            "policy_role",
            "external_team1",
            "admin_role",
            "cluster_role",
            "probe_role_a",
            "probe_role_b",
            "capped_role",
        ]:
            admin(f"CREATE ROLE IF NOT EXISTS {role}")
            instance2.query(
                f"CREATE ROLE IF NOT EXISTS {role}",
                user="admin_user",
                password="admin_password",
            )
        admin("CREATE ROLE IF NOT EXISTS only_node1_role")
        admin(
            "CREATE TABLE IF NOT EXISTS default.protected (x UInt64) ENGINE = MergeTree ORDER BY x"
        )
        admin("INSERT INTO default.protected VALUES (1), (2), (3)")
        admin("GRANT SELECT ON default.protected TO reader")
        yield cluster
    finally:
        cluster.shutdown()


def test_http_directory_registered_on_every_node(started_cluster):
    for node in [instance, instance2, instance3, instance4]:
        directories = node.query(
            "SELECT count() FROM system.user_directories WHERE type = 'http'",
            user="admin_user",
            password="admin_password",
        ).strip()
        assert directories == "1"


def test_node3_directory_order_http_before_users_xml(started_cluster):
    # Guards Blocker 1: node3 must have the http directory BEFORE users.xml, and NOT an
    # implicit users_config ahead of it. system.user_directories is ordered by precedence;
    # assert http precedes any users.xml/users_config storage and appears exactly once each.
    rows = instance3.query(
        "SELECT type FROM system.user_directories ORDER BY precedence",
        user="admin_user",
        password="admin_password",
    ).split()
    assert "http" in rows, rows
    users_types = [t for t in rows if t in ("users.xml", "users_config", "users_xml")]
    assert users_types, rows
    assert rows.index("http") < rows.index(users_types[0]), rows
    # Exactly one users.xml storage (the explicit one) — the implicit users_config was removed.
    assert len(users_types) == 1, rows


def test_unknown_user_materialized_in_memory_only(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="http_user", password=GOOD_PASSWORD
        ).strip()
        == "http_user"
    )
    # Materialized as an ephemeral user in the http storage.
    assert (
        admin("SELECT storage FROM system.users WHERE name = 'http_user'").strip()
        == "http"
    )
    # No persistent grants on the cached user: roles are session-scoped.
    assert admin("SHOW GRANTS FOR http_user").strip() == ""


def test_allowed_role_becomes_effective_and_authorizes(started_cluster):
    roles = instance.query(
        "SELECT arrayJoin(currentRoles())", user="http_user", password=GOOD_PASSWORD
    ).strip()
    assert roles == "reader"
    result = instance.query(
        "SELECT count() FROM default.protected",
        user="http_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert result == "3"
    # A helper user with no roles cannot read the protected table.
    error = instance.query_and_get_error(
        "SELECT count() FROM default.protected",
        user="norole_user",
        password=GOOD_PASSWORD,
    )
    assert "Not enough privileges" in error or "ACCESS_DENIED" in error


def test_roles_absent_and_empty_mean_empty_role_set(started_cluster):
    assert (
        instance.query(
            "SELECT currentRoles()", user="norole_user", password=GOOD_PASSWORD
        ).strip()
        == "[]"
    )
    assert (
        instance.query(
            "SELECT currentRoles()", user="emptyroles_user", password=GOOD_PASSWORD
        ).strip()
        == "[]"
    )


def test_session_settings_from_response(started_cluster):
    value = instance.query(
        "SELECT getSetting('max_threads')", user="settings_user", password=GOOD_PASSWORD
    ).strip()
    assert value == "4"


def test_wrong_password_fails_closed(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="http_user", password="wrong_password"
    )


def test_membership_change_between_authentications(started_cluster):
    # Two authentications of the same username with different helper-returned
    # role sets: each session gets exactly its own set, and the cached user
    # never accumulates grants.
    roles_a = instance.query(
        "SELECT arrayJoin(currentRoles())", user="dual_user", password="password_a"
    ).strip()
    assert roles_a == "role_a"
    roles_b = instance.query(
        "SELECT arrayJoin(currentRoles())", user="dual_user", password="password_b"
    ).strip()
    assert roles_b == "role_b"
    roles_a_again = instance.query(
        "SELECT arrayJoin(currentRoles())", user="dual_user", password="password_a"
    ).strip()
    assert roles_a_again == "role_a"
    assert admin("SHOW GRANTS FOR dual_user").strip() == ""


def test_two_simultaneous_sessions_keep_own_roles(started_cluster):
    # ADR additional test 1: two SIMULTANEOUS sessions of the same username with
    # different role sets stay independent. Two distinct named sessions interleaved:
    # touching session B must not change session A's effective roles.
    def http_roles(password, session):
        return instance.http_query(
            "SELECT arrayJoin(currentRoles())",
            user="dual_user",
            password=password,
            params={"session_id": session},
        ).strip()

    assert http_roles("password_a", "sess_sim_a") == "role_a"
    assert http_roles("password_b", "sess_sim_b") == "role_b"
    # Session A still has role_a after session B was used.
    assert http_roles("password_a", "sess_sim_a") == "role_a"


def test_prefix_delegation(started_cluster):
    roles = instance.query(
        "SELECT arrayJoin(currentRoles())", user="prefix_user", password=GOOD_PASSWORD
    ).strip()
    assert roles == "external_team1"


def test_existing_but_disallowed_role_fails(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="disallowed_role_user", password=GOOD_PASSWORD
    )


def test_one_allowed_plus_one_disallowed_fails_whole_attempt(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="mixed_roles_user", password=GOOD_PASSWORD
    )


def test_unknown_role_fails(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="unknown_role_user", password=GOOD_PASSWORD
    )


def test_malformed_response_fails(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="malformed_json_user", password=GOOD_PASSWORD
    )
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="bad_roles_type_user", password=GOOD_PASSWORD
    )


def test_valid_until(started_cluster):
    # Expired (past) and malformed (negative) valid_until fail the attempt.
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="expired_user", password=GOOD_PASSWORD
    )
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="negative_vu_user", password=GOOD_PASSWORD
    )
    # A future valid_until authenticates.
    assert (
        instance.query(
            "SELECT 1", user="future_vu_user", password=GOOD_PASSWORD
        ).strip()
        == "1"
    )


def test_failed_auth_does_not_materialize_user(started_cluster):
    instance.query_and_get_error(
        "SELECT 1", user="unknown_role_user", password=GOOD_PASSWORD
    )
    assert (
        admin(
            "SELECT count() FROM system.users WHERE name = 'unknown_role_user'"
        ).strip()
        == "0"
    )


def test_ephemeral_user_cannot_get_persistent_grants(started_cluster):
    # The entire session-scoped model rests on the storage being read-only:
    # nobody can attach persistent grants or alter the ephemeral user.
    instance.query(
        "SELECT 1", user="http_user", password=GOOD_PASSWORD
    )  # ensure materialized
    for ddl in [
        "GRANT reader TO http_user",
        "GRANT SELECT ON default.protected TO http_user",
        # DEFAULT ROLE NONE (rather than a named role) so this DDL reaches the
        # storage's readonly write path instead of failing earlier with
        # SET_NON_GRANTED_ROLE: InterpreterSetRoleQuery::updateUserSetDefaultRoles
        # validates that a role is already granted to the user before any storage
        # write is attempted, and the ephemeral http_user can never actually have
        # a role granted (that's the very readonly behavior this test checks).
        "ALTER USER http_user DEFAULT ROLE NONE",
    ]:
        error = instance.query_and_get_error(
            ddl, user="admin_user", password="admin_password"
        )
        assert (
            "readonly" in error.lower()
            or "read-only" in error.lower()
            or "ACCESS_STORAGE_READONLY" in error
        )
    assert admin("SHOW GRANTS FOR http_user").strip() == ""


def test_empty_password_fails(started_cluster):
    # Borrowed from the LDAP directory suite (test_authentication_fail): an empty
    # password must fail like any wrong password, fail-closed.
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="http_user", password=""
    )


def test_role_dropped_and_recreated(started_cluster):
    # Borrowed from the LDAP directory suite (test_role_mapping): roles are resolved
    # by name at every authentication, not by a cached UUID. While the role does not
    # exist, authentication fails closed (unlike LDAP, which skips the role); after
    # re-creation, authentication succeeds with the new role entity.
    admin("DROP ROLE external_team1")
    try:
        assert "Authentication failed" in instance.query_and_get_error(
            "SELECT 1", user="prefix_user", password=GOOD_PASSWORD
        )
    finally:
        admin("CREATE ROLE external_team1")
    roles = instance.query(
        "SELECT arrayJoin(currentRoles())", user="prefix_user", password=GOOD_PASSWORD
    ).strip()
    assert roles == "external_team1"


def test_role_attached_row_policy_and_profile_apply(started_cluster):
    admin(
        "CREATE TABLE IF NOT EXISTS default.policed (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    admin("INSERT INTO default.policed VALUES (1), (2), (3)")
    admin("GRANT SELECT ON default.policed TO policy_role")
    admin(
        "CREATE ROW POLICY IF NOT EXISTS p1 ON default.policed USING x < 3 TO policy_role"
    )
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS role_profile SETTINGS max_result_rows = 1000"
    )
    admin("ALTER ROLE policy_role ADD PROFILES 'role_profile'")
    result = instance.query(
        "SELECT count() FROM default.policed",
        user="rowpolicy_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert result == "2"
    value = instance.query(
        "SELECT getSetting('max_result_rows')",
        user="rowpolicy_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert value == "1000"
