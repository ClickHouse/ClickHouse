import os
import shlex

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


def admin2(query, **kwargs):
    return instance2.query(
        query, user="admin_user", password="admin_password", **kwargs
    )


def query4_local(query, user=None, password=None, nothrow=False):
    # ADAPTATION (Task 8): node4's http directory restricts `networks` to `127.0.0.1/32`.
    # `instance4.query` (the normal helper) runs `clickhouse-client` as a SEPARATE process
    # on the test-runner host, connecting to node4 over the docker bridge network — node4
    # sees that connection's address as the container's own docker-network IP, never
    # `127.0.0.1`, so a query issued that way is always rejected by this networks policy
    # regardless of credentials. To make a query actually originate from node4's own
    # loopback (as the `networks` policy requires), run `clickhouse-client` INSIDE node4's
    # container via `exec_in_container`, pointed at `--host 127.0.0.1`. stderr is
    # redirected into stdout (`2>&1`) because `exec_in_container` with `nothrow=True`
    # only returns stdout.
    cmd = "/usr/bin/clickhouse client --host 127.0.0.1"
    if user is not None:
        cmd += f" --user {shlex.quote(user)}"
    if password is not None:
        cmd += f" --password {shlex.quote(password)}"
    cmd += f" --query {shlex.quote(query)}"
    return instance4.exec_in_container(["bash", "-c", cmd + " 2>&1"], nothrow=nothrow)


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


def test_404_falls_through_to_later_storage(started_cluster):
    # On node3 the http directory is configured BEFORE users.xml. admin_user is unknown
    # to the helper (404), so authentication must fall through to users.xml and succeed.
    # Use a fresh admin credential name to make the mock-consulted assertion unambiguous.
    assert (
        instance3.query(
            "SELECT currentUser()", user="admin_user", password="admin_password"
        ).strip()
        == "admin_user"
    )
    # Non-vacuity guard (Blocker 1): prove the http directory was actually consulted first
    # and returned 404 — i.e. the mock saw admin_user. If users.xml preceded http, the mock
    # would never see admin_user and this assertion fails.
    seen = instance3.exec_in_container(
        ["bash", "-c", "curl -s 'http://localhost:8000/seen?user=admin_user'"]
    ).strip()
    assert seen == "1", "http directory was not consulted before users.xml"


def test_wrong_password_does_not_fall_through(started_cluster):
    # On node3, shadowed_user is known to BOTH the helper (http directory, first) and
    # users.xml (second, password xml_password). The helper answers 401 for xml_password,
    # which must fail closed WITHOUT trying users.xml — the key no-fallback row of the matrix.
    assert "Authentication failed" in instance3.query_and_get_error(
        "SELECT 1", user="shadowed_user", password="xml_password"
    )
    # The helper's own password works through the http directory.
    assert (
        instance3.query(
            "SELECT currentUser()", user="shadowed_user", password=GOOD_PASSWORD
        ).strip()
        == "shadowed_user"
    )


def test_default_profile_resolved_late_and_fail_closed(started_cluster):
    admin4 = lambda q: instance4.query(q, user="admin_user", password="admin_password")
    # node4's directory declares default_profile=sql_profile, which does not exist yet:
    # the very first materialization must fail closed.
    assert "Authentication failed" in query4_local(
        "SELECT 1", user="aux_user", password=GOOD_PASSWORD, nothrow=True
    )
    # After the profile is created (in a SQL-driven storage, i.e. after this directory
    # was constructed), materialization succeeds and the profile applies.
    admin4(
        "CREATE SETTINGS PROFILE IF NOT EXISTS sql_profile SETTINGS max_rows_to_read = 12345"
    )
    value = query4_local(
        "SELECT getSetting('max_rows_to_read')", user="aux_user", password=GOOD_PASSWORD
    ).strip()
    assert value == "12345"


def test_networks_allow_localhost(started_cluster):
    # node4's directory is restricted to `127.0.0.1/32`. `instance4.query` (the ordinary
    # helper) issues `clickhouse-client` from the test-runner host over the docker bridge
    # network, which node4 never sees as `127.0.0.1` — so this test must use
    # `query4_local`, which runs `clickhouse-client` INSIDE node4's own container against
    # its own loopback. The rejection path from a remote client is covered in Task 10
    # (`test_networks_reject_remote_client`).
    assert (
        query4_local("SELECT 1", user="aux_user", password=GOOD_PASSWORD).strip() == "1"
    )


def test_response_settings_override_profile_value(started_cluster):
    # ADR additional test 8: response settings are applied after profile initialization
    # and override the profile-provided value (sql_profile sets max_rows_to_read=12345,
    # the response returns 777). Requires sql_profile to exist — runs after
    # test_default_profile_resolved_late_and_fail_closed created it; create it here
    # too with IF NOT EXISTS to stay order-independent.
    instance4.query(
        "CREATE SETTINGS PROFILE IF NOT EXISTS sql_profile SETTINGS max_rows_to_read = 12345",
        user="admin_user",
        password="admin_password",
    )
    value = query4_local(
        "SELECT getSetting('max_rows_to_read')",
        user="aux_override_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert value == "777"


def test_helper_down_fails_closed(started_cluster):
    # Infrastructure failure is fail-closed (never a fallthrough). Kill node4's mock
    # server, authenticate, then restart the mock for later tests.
    instance4.exec_in_container(
        ["bash", "-c", "pkill -f http_auth_server.py"], user="root"
    )
    try:
        assert "Authentication failed" in query4_local(
            "SELECT 1", user="aux_user", password=GOOD_PASSWORD, nothrow=True
        )
    finally:
        start_mock_server(instance4)


def test_default_profile_dropped_and_recreated_for_cached_user(started_cluster):
    # Regression for the cached-user default_profile blocker found in review: getOrCreateUser
    # must re-resolve default_profile on every call, not just at first materialization, and
    # fail closed for an ALREADY-CACHED user too. Sequence:
    #   authenticate (cached, sees 12345) -> drop profile -> authenticate fails closed ->
    #   recreate same name with a different value -> authenticate succeeds, sees the new value.
    # This directly exercises the failure-matrix contract for a materialized user, which
    # test_default_profile_resolved_late_and_fail_closed does not: that test only covers a
    # missing profile before the FIRST materialization.
    admin4 = lambda q: instance4.query(q, user="admin_user", password="admin_password")
    admin4(
        "CREATE SETTINGS PROFILE IF NOT EXISTS sql_profile SETTINGS max_rows_to_read = 12345"
    )
    # Ensure aux_user is already cached under the current sql_profile UUID.
    assert (
        query4_local(
            "SELECT getSetting('max_rows_to_read')",
            user="aux_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "12345"
    )

    admin4("DROP SETTINGS PROFILE sql_profile")
    # The cached user must fail closed, not silently keep authenticating on stale settings.
    assert "Authentication failed" in query4_local(
        "SELECT 1", user="aux_user", password=GOOD_PASSWORD, nothrow=True
    )

    # Recreate under the same name but a new UUID and a different value.
    admin4("CREATE SETTINGS PROFILE sql_profile SETTINGS max_rows_to_read = 54321")
    assert (
        query4_local(
            "SELECT getSetting('max_rows_to_read')",
            user="aux_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "54321"
    )
    # This is the last test in this file that depends on sql_profile's specific value;
    # later tests only use sql_profile's existence, not its value.


def test_response_settings_apply(started_cluster):
    value = instance.query(
        "SELECT getSetting('max_threads')",
        user="profileclash_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert value == "7"


def test_local_user_shadows_helper_user(started_cluster):
    # On node (users.xml first), local_user exists in users.xml AND in the helper.
    # The xml password works; the helper password does not, because users.xml
    # finds the user and fails closed on a wrong password without falling through.
    assert (
        instance.query(
            "SELECT currentUser()", user="local_user", password="local_password"
        ).strip()
        == "local_user"
    )
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="local_user", password=GOOD_PASSWORD
    )
    assert (
        admin("SELECT storage FROM system.users WHERE name = 'local_user'").strip()
        == "users_xml"
    )


def test_server_errors_fail_closed(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="err500_user", password=GOOD_PASSWORD
    )
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="err429_user", password=GOOD_PASSWORD
    )


def test_totally_unknown_user_fails(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="ghost_user", password=GOOD_PASSWORD
    )


def test_max_cached_users_bound(started_cluster):
    # node3's directory allows at most 3 materialized users (soft bound; this test is
    # sequential, so no overshoot occurs). One slot is taken by shadowed_user if an
    # earlier test materialized it; count from system.users first and fill up to the
    # bound deterministically.
    admin3 = lambda q: instance3.query(q, user="admin_user", password="admin_password")
    already = int(
        admin3("SELECT count() FROM system.users WHERE storage = 'http'").strip()
    )
    for i in range(3 - already):
        assert (
            instance3.query(
                "SELECT 1", user=f"cache_user_{i}", password=GOOD_PASSWORD
            ).strip()
            == "1"
        )
    # The next new username is rejected...
    assert "Authentication failed" in instance3.query_and_get_error(
        "SELECT 1", user="cache_user_9", password=GOOD_PASSWORD
    )
    # ...but an already materialized user keeps authenticating even at the bound.
    a_cached_user = admin3(
        "SELECT name FROM system.users WHERE storage = 'http' LIMIT 1"
    ).strip()
    assert (
        instance3.query("SELECT 1", user=a_cached_user, password=GOOD_PASSWORD).strip()
        == "1"
    )
    # Sequentially, the cache holds exactly the bound (no assertion of strictness under
    # concurrency — the bound is documented as approximate).
    assert (
        int(admin3("SELECT count() FROM system.users WHERE storage = 'http'").strip())
        == 3
    )


def test_concurrent_first_authentication_converges(started_cluster):
    import threading

    results = []

    def login():
        results.append(
            instance.query(
                "SELECT currentUser()",
                user="http_user_concurrent",
                password=GOOD_PASSWORD,
            ).strip()
        )

    threads = [threading.Thread(target=login) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == ["http_user_concurrent"] * 8
    # Exactly one cached entity.
    assert (
        admin(
            "SELECT count() FROM system.users WHERE name = 'http_user_concurrent'"
        ).strip()
        == "1"
    )


def test_distinct_users_authenticate_concurrently(started_cluster):
    # Proves remote HTTP I/O is NOT serialized by a directory-wide lock: N distinct
    # usernames authenticate at once, and the mock's handler barriers all N requests
    # before releasing any. With a directory-wide lock the second request never reaches
    # the mock while the first is blocked, so the barrier never fills and the mock times
    # out (requests fail). Without such a lock, all N arrive and the barrier releases.
    import threading

    n = 4
    results = []

    def login(i):
        try:
            results.append(
                instance.query(
                    "SELECT 1", user=f"barrier_user_{i}", password=GOOD_PASSWORD
                ).strip()
            )
        except Exception as e:  # noqa: BLE001
            results.append(f"ERR:{e}")

    threads = [threading.Thread(target=login, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == ["1"] * n, results


def test_distributed_query_propagates_helper_roles(started_cluster):
    # Mirror the LDAP push test: role grants and the local table exist on both nodes,
    # data only on node2, a Distributed table on the initiator.
    admin("GRANT SELECT ON default.* TO cluster_role")
    admin2("GRANT SELECT ON default.* TO cluster_role")
    admin(
        "CREATE TABLE IF NOT EXISTS default.local_table (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    admin2(
        "CREATE TABLE IF NOT EXISTS default.local_table (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    admin2("INSERT INTO default.local_table VALUES (42)")
    admin(
        "CREATE TABLE IF NOT EXISTS default.distributed_table AS default.local_table "
        "ENGINE = Distributed(test_cluster, default, local_table)"
    )
    # distributed_user gets cluster_role from the helper on node; the role must be
    # effective on node2 via interserver propagation, where the user is materialized
    # through AlwaysAllowCredentials without an HTTP request.
    result = instance.query(
        "SELECT count() FROM default.distributed_table",
        user="distributed_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert result == "1"
    # Materialized on node2 by the interserver path.
    assert (
        admin2(
            "SELECT storage FROM system.users WHERE name = 'distributed_user'"
        ).strip()
        == "http"
    )


def test_receiving_node_does_not_contact_its_http_server(started_cluster):
    # The central interserver contract: on the AlwaysAllowCredentials path the receiving
    # node must NOT call its own HTTP auth server. Prove it directly by stopping node2's
    # mock, then running a distributed query as a FRESH (uncached-on-node2) helper user.
    # The initiator (node) still authenticates it against node's live mock; node2 must
    # materialize it via AlwaysAllowCredentials with node2's mock DOWN and the query must
    # still succeed. (interserver_user is delegated cluster_role by node's mock.)
    admin2("GRANT SELECT ON default.* TO cluster_role")  # idempotent
    instance2.exec_in_container(
        ["bash", "-c", "pkill -f http_auth_server.py"], user="root"
    )
    try:
        result = instance.query(
            "SELECT count() FROM default.distributed_table",
            user="interserver_user",
            password=GOOD_PASSWORD,
        ).strip()
        assert result == "1"
        assert (
            admin2(
                "SELECT storage FROM system.users WHERE name = 'interserver_user'"
            ).strip()
            == "http"
        )
    finally:
        start_mock_server(instance2)


def test_distributed_query_through_view_preserves_roles(started_cluster):
    # Context-copy regression (cf. the external-role loss history): the same
    # distributed read, but through a normal VIEW on the initiator.
    admin(
        "CREATE VIEW IF NOT EXISTS default.distributed_view AS SELECT * FROM default.distributed_table"
    )
    admin("GRANT SELECT ON default.distributed_view TO cluster_role")
    result = instance.query(
        "SELECT count() FROM default.distributed_view",
        user="distributed_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert result == "1"


def test_local_view_context_copy_preserves_roles(started_cluster):
    # Purely local deferred/context-copy path: a VIEW over a role-protected table.
    admin(
        "CREATE VIEW IF NOT EXISTS default.protected_view AS SELECT * FROM default.protected"
    )
    admin("GRANT SELECT ON default.protected_view TO reader")
    result = instance.query(
        "SELECT count() FROM default.protected_view",
        user="http_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert result == "3"


def test_distributed_query_fails_closed_on_role_unknown_remotely(started_cluster):
    # halfcluster_user returns [reader, only_node1_role]; only_node1_role exists only on
    # `node`. These roles travel via ClientInfo.current_roles (Context::getCurrentRoles,
    # which includes external roles). The receiver resolves that set in
    # makeQueryContextImpl and FAILS CLOSED on any size mismatch (ACCESS_DENIED) — it does
    # NOT drop the unknown role and run with the resolvable remainder. This is the
    # current_roles path, verified fail-closed on origin/master; distinct from the legacy
    # external_roles (granted_roles) push, which does silently drop unknown names.
    admin("GRANT SELECT ON default.* TO only_node1_role")
    error = instance.query_and_get_error(
        "SELECT count() FROM default.distributed_table",
        user="halfcluster_user",
        password=GOOD_PASSWORD,
    )
    assert "current roles" in error or "ACCESS_DENIED" in error


def test_networks_reject_remote_client(started_cluster):
    # node4's directory allows only 127.0.0.1/32. A client connecting from node2's
    # address must be rejected (the fail-closed `networks` row of the matrix;
    # complements Task 8's allow test).
    # ADAPTATION: the brief's literal command invokes a `clickhouse-client` binary, which
    # this image does not provide (`bash: line 1: clickhouse-client: command not found`,
    # verified by running the test). Use the same `/usr/bin/clickhouse client` multi-call
    # invocation as `query4_local` above, which this suite already established as the
    # working form in this image.
    output = instance2.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse client --host node4 --user aux_user --password good_password --query 'SELECT 1' 2>&1 || true",
        ]
    )
    assert "Authentication failed" in output or "not allowed" in output
