import os
import shlex
import threading

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
    # node4's http directory restricts `networks` to `127.0.0.1/32`.
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


def helper_request_count(node, user):
    return int(
        node.exec_in_container(
            ["curl", "-s", f"http://localhost:8000/count?user={user}"]
        ).strip()
    )


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
            "limit_role_a",
            "limit_role_b",
            "external_definer",
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
    # Reattachment to a named `session_id` rebinds roles per
    # request (see "Replace authentication-scoped external roles on named-session
    # reattachment"), so this is a rebind smoke test, not proof of concurrent-session
    # isolation: each request's roles follow its own authentication, and touching session
    # B does not alter what session A's NEXT authenticated request sees. It does not show
    # the two sessions holding independent state concurrently while both are in use — that
    # shape (two connections open and doing work at once, each keeping its own lifetime) is
    # covered by `test_two_established_sessions_expire_independently`'s native-connection
    # form, for expiry rather than roles.
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
    # its own loopback. The rejection path from a remote client is covered
    # (`test_networks_reject_remote_client`).
    assert (
        query4_local("SELECT 1", user="aux_user", password=GOOD_PASSWORD).strip() == "1"
    )


def test_response_settings_override_profile_value(started_cluster):
    # Response settings are applied after profile initialization
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
    # complements the allow test above).
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


def http_sess(sql, user, password, session):
    return instance.http_query(
        sql, user=user, password=password, params={"session_id": session}
    )


def test_named_session_rebind_replaces_roles(started_cluster):
    session = "sess_rebind_roles"
    assert (
        http_sess(
            "SELECT arrayJoin(currentRoles())", "dual_user", "password_a", session
        ).strip()
        == "role_a"
    )
    # Reattachment under a different authentication replaces the role set.
    assert (
        http_sess(
            "SELECT arrayJoin(currentRoles())", "dual_user", "password_b", session
        ).strip()
        == "role_b"
    )
    # Replacement by an empty set also works.
    assert (
        http_sess(
            "SELECT currentRoles()", "dual_user", "password_none", session
        ).strip()
        == "[]"
    )


def test_named_session_preserves_set_state(started_cluster):
    session = "sess_persist"
    http_sess("SELECT 1", "dual_user", "password_a", session)
    http_sess("SET max_threads = 12", "dual_user", "password_a", session)
    assert (
        http_sess(
            "SELECT getSetting('max_threads')", "dual_user", "password_a", session
        ).strip()
        == "12"
    )
    # Reattachment (even under a different authentication) must not reset SET state.
    assert (
        http_sess(
            "SELECT getSetting('max_threads')", "dual_user", "password_b", session
        ).strip()
        == "12"
    )


def test_named_session_applies_auth_settings_at_creation_only(started_cluster):
    session = "sess_auth_settings"
    assert (
        http_sess(
            "SELECT getSetting('max_threads')", "settings_user", GOOD_PASSWORD, session
        ).strip()
        == "4"
    )
    http_sess("SET max_threads = 12", "settings_user", GOOD_PASSWORD, session)
    # The next reattachment carries the same auth settings but must not stomp SET state.
    assert (
        http_sess(
            "SELECT getSetting('max_threads')", "settings_user", GOOD_PASSWORD, session
        ).strip()
        == "12"
    )


def test_legacy_http_user_named_session_settings(started_cluster):
    # Regression scope for the behavior change: a pre-created user with IDENTIFIED WITH
    # HTTP now also gets its auth-server settings at named-session creation.
    admin(
        "CREATE USER IF NOT EXISTS legacy_settings_user IDENTIFIED WITH HTTP SERVER 'main_server' SCHEME 'BASIC'"
    )
    session = "sess_legacy"
    assert (
        http_sess(
            "SELECT getSetting('max_threads')",
            "legacy_settings_user",
            GOOD_PASSWORD,
            session,
        ).strip()
        == "4"
    )
    http_sess("SET max_threads = 12", "legacy_settings_user", GOOD_PASSWORD, session)
    assert (
        http_sess(
            "SELECT getSetting('max_threads')",
            "legacy_settings_user",
            GOOD_PASSWORD,
            session,
        ).strip()
        == "12"
    )


def test_named_session_role_profile_contract(started_cluster):
    # Contract test (decided: login-time role-profile state). Role-derived settings and
    # constraints are established when the named session is CREATED and are NOT rebuilt
    # when a reattachment rebinds the role set. Privileges/row policies DO follow the
    # rebind (covered by test_named_session_rebind_replaces_roles).
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS cap_profile SETTINGS max_threads MAX 4"
    )
    admin("ALTER ROLE probe_role_b ADD PROFILES 'cap_profile'")
    # Direction 1: a session CREATED under probe_role_b gets the constraint...
    session_b = "sess_contract_created_b"
    error = instance.http_query_and_get_error(
        "SET max_threads = 16",
        user="probe_user",
        password="password_b",
        params={"session_id": session_b},
    )
    assert "max_threads" in error  # constraint from creation-time role binds
    # ...and the creation-time constraint PERSISTS after rebinding away to probe_role_a
    # (role-derived constraints are creation-time named-session state).
    error = instance.http_query_and_get_error(
        "SET max_threads = 16",
        user="probe_user",
        password="password_a",
        params={"session_id": session_b},
    )
    assert "max_threads" in error
    # Direction 2: a session CREATED under probe_role_a (no constraint) keeps its
    # looser settings state after a rebind to probe_role_b — documented behavior.
    session_a = "sess_contract_created_a"
    http_sess("SELECT 1", "probe_user", "password_a", session_a)
    http_sess("SET max_threads = 16", "probe_user", "password_a", session_a)
    http_sess(
        "SET max_threads = 8", "probe_user", "password_b", session_a
    )  # must succeed
    assert (
        http_sess(
            "SELECT getSetting('max_threads')", "probe_user", "password_b", session_a
        ).strip()
        == "8"
    )


def test_two_established_sessions_expire_independently(started_cluster):
    # Two NATIVE (persistent TCP) connections authenticate ONCE
    # each with different ABSOLUTE deadlines and are held open CONCURRENTLY. Per-query
    # expiry enforcement (Session::checkIfUserIsStillValid on TCP) then applies to each
    # connection independently — the short one must expire while the long one keeps working,
    # proving neither authentication modifies the other's lifetime. Both clients start at
    # the same time (background processes) and each sends statements spaced by client-side
    # sleeps so their last statements run after the short deadline.
    # ADAPTATION: this image does not provide a `clickhouse-client` binary (verified by
    # other tests in this suite, e.g. `test_networks_reject_remote_client`); use
    # `/usr/bin/clickhouse client` instead. `--multiquery` also errors on this version
    # (multi-statement queries are the default now), so it is dropped.
    import time

    now = int(time.time())
    mq = "SELECT 1; SELECT sleep(3); SELECT sleep(3); SELECT 1"
    # Launch both concurrently; capture each client's combined output to a file.
    script = (
        f"/usr/bin/clickhouse client --user expiry_user --password until_{now + 4} "
        f"--query '{mq}' > /tmp/short.out 2>&1 & "
        f"/usr/bin/clickhouse client --user expiry_user --password until_{now + 3600} "
        f"--query '{mq}' > /tmp/long.out 2>&1 & "
        "wait"
    )
    instance.exec_in_container(["bash", "-c", script])
    short = instance.exec_in_container(["bash", "-c", "cat /tmp/short.out"])
    long = instance.exec_in_container(["bash", "-c", "cat /tmp/long.out"])
    assert "USER_EXPIRED" in short or "expired" in short.lower(), short
    assert "USER_EXPIRED" not in long, long


def test_named_session_expiry_replaced_for_async_insert(started_cluster):
    # Named-session expiry replacement, DETERMINISTIC deferred vehicle. A plain
    # reattach cannot prove replacement — every HTTP request re-authenticates, so the
    # request's own user_authenticated_with already carries the new deadline. An async
    # INSERT is genuinely deferred: AsynchronousInsertQueue captures
    # query_context->getAuthenticationValidUntil() at enqueue (the COPIED named-Context
    # field, src/Interpreters/AsynchronousInsertQueue.cpp:631) and re-checks it at flush,
    # throwing USER_EXPIRED if now > deadline (same file, ~line 1106).
    #
    # Create the named session under a SHORT deadline, reattach under a LONG one (which must
    # WRITE the new expiry into the reused named Context via setAuthenticationValidUntil),
    # then enqueue an async insert whose flush lands AFTER the short deadline. Setter present
    # -> captured long deadline -> flush ok, row lands. Setter missing -> captured stale
    # short deadline -> flush throws USER_EXPIRED, row does not land.
    import time

    admin(
        "CREATE TABLE IF NOT EXISTS default.async_target (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    admin("GRANT INSERT ON default.async_target TO reader")
    now = int(time.time())
    session = "sess_async_expiry"
    http_sess("SELECT 1", "expiry_user", f"until_{now + 5}", session)  # create, short
    http_sess(
        "SELECT 1", "expiry_user", f"until_{now + 3600}", session
    )  # reattach, long
    # wait_for_async_insert=1 blocks until flush; the busy timeout pushes the flush past the
    # 5s short deadline. async_insert_use_adaptive_busy_timeout defaults to true, in which
    # case async_insert_busy_timeout_max_ms is NOT the flush delay — it is only the ceiling
    # the adaptive algorithm ramps toward from async_insert_busy_timeout_min_ms (default
    # 50ms); the real delay would then be nondeterministic and could flush well before the
    # short deadline, making this test vacuous. Disable adaptive behavior so
    # async_insert_busy_timeout_max_ms is used directly (confirmed against
    # AsynchronousInsertQueue.cpp: the non-adaptive branch returns max_ms as-is).
    # http_query raises on any server error, so a missing setter (flush-time USER_EXPIRED)
    # fails this test with the actual server error; http_query_and_get_error would be wrong
    # here because it raises when the query SUCCEEDS. Explicit timeout: this request
    # deliberately takes ~7 seconds.
    started = time.monotonic()
    instance.http_query(
        "INSERT INTO default.async_target SETTINGS async_insert=1, wait_for_async_insert=1, "
        "async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_max_ms=7000 VALUES (1)",
        user="expiry_user",
        password=f"until_{now + 3600}",
        params={"session_id": session},
        # ADAPTATION: http_query defaults to GET unless `data` is passed (see
        # helpers/cluster.py: `method = "POST" if data else "GET"`), and INSERT over GET is
        # rejected as readonly. Force POST explicitly, matching the precedent in
        # test_insert_over_http_query_log/test.py.
        method="POST",
        timeout=20,
    )
    elapsed = time.monotonic() - started
    # The request must have genuinely waited past the short (5s) deadline for the flush-time
    # expiry re-check to be what's under test, rather than the setter having merely made an
    # already-fast flush pass. A future change to the default busy timeout must fail this
    # test rather than pass it vacuously.
    assert elapsed > 5, (
        f"async insert returned in {elapsed:.1f}s, too fast to have exercised "
        "the flush-time expiry re-check"
    )
    assert instance.query("SELECT count() FROM default.async_target").strip() == "1"


def test_failed_named_session_init_not_reusable(started_cluster):
    # Regression for the named-session cleanup guard. Creating a named session fails when a
    # returned setting violates a constraint from the returned role's profile
    # (checkSettingsConstraints throws AFTER acquireSession has published the session).
    # The failed session must not remain reusable: a later request with the same
    # session_id must get a FRESH session that applies its own auth settings, not the
    # half-initialized one (which took the reuse branch and applied no settings).
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS guard_cap_profile SETTINGS max_threads MAX 4"
    )
    admin("ALTER ROLE capped_role ADD PROFILES 'guard_cap_profile'")
    session = "sess_guard"
    # Request 1: fails during named-session creation (max_threads=16 vs MAX 4).
    err = instance.http_query_and_get_error(
        "SELECT 1",
        user="guard_user",
        password="cause_fail",
        params={"session_id": session},
    )
    assert err
    # Request 2: same session_id, a VALID authentication returning max_result_rows=555.
    # If the failed session were left reusable, request 2 would take the reuse branch and
    # NOT apply auth settings, so max_result_rows would not be 555. The guard removed the
    # failed session, so request 2 creates a fresh one and applies its settings.
    value = instance.http_query(
        "SELECT getSetting('max_result_rows')",
        user="guard_user",
        password="valid",
        params={"session_id": session},
    ).strip()
    assert value == "555"


def _event_value(event_name):
    # system.events is cumulative since server startup and has no row for an event that
    # has never fired, so a missing row is treated as 0.
    result = admin(
        f"SELECT value FROM system.events WHERE event = '{event_name}'"
    ).strip()
    return int(result) if result else 0


def _metric_value(metric_name):
    result = admin(
        f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
    ).strip()
    return int(result) if result else 0


def test_metrics(started_cluster):
    # Delta-based, not absolute: system.events/system.metrics are cumulative since server
    # startup and this test runs last among many sharing one cluster, so an absolute
    # `> 0` assertion would pass regardless of what this test does. Single-threaded and
    # nothing else runs concurrently in this suite, so a snapshot-then-act-then-snapshot
    # pair is race-free.
    def snapshot():
        return {
            "requests": _event_value("HTTPUserDirectoryAuthRequests"),
            "failures": _event_value("HTTPUserDirectoryAuthFailures"),
            "created": _event_value("HTTPUserDirectoryUsersCreated"),
            "cached": _metric_value("HTTPUserDirectoryCachedUsers"),
        }

    # 1. A brand-new username, first (successful) authentication: materializes a new
    # cached user.
    before = snapshot()
    instance.query("SELECT 1", user="metrics_user", password=GOOD_PASSWORD)
    after = snapshot()
    assert after["requests"] == before["requests"] + 1
    assert after["created"] == before["created"] + 1
    assert after["cached"] == before["cached"] + 1
    assert after["failures"] == before["failures"]

    # 2. Same username, second authentication: cache hit, no new materialization.
    before = snapshot()
    instance.query("SELECT 1", user="metrics_user", password=GOOD_PASSWORD)
    after = snapshot()
    assert after["requests"] == before["requests"] + 1
    assert after["created"] == before["created"]
    assert after["cached"] == before["cached"]
    assert after["failures"] == before["failures"]

    # 3. Same username, wrong password: a Basic external-authentication failure.
    before = snapshot()
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="metrics_user", password="wrong"
    )
    after = snapshot()
    assert after["requests"] == before["requests"] + 1
    assert after["failures"] == before["failures"] + 1

    # 4. A username the mock server does not know: 404 (UserNotFound) fallthrough, then
    # "Authentication failed" overall since `users.xml` (the implicit `users_config`
    # storage) precedes `http`, which is the last storage on `node`, so
    # `throw_if_user_not_exists` is true for it. Not a failure:
    # HTTPUserDirectoryAuthFailures must stay unchanged.
    before = snapshot()
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="metrics_ghost_user", password=GOOD_PASSWORD
    )
    after = snapshot()
    assert after["requests"] == before["requests"] + 1
    assert after["failures"] == before["failures"]


def test_named_session_refused_by_session_limit_not_reusable(started_cluster):
    # Admission (`trackSession`, USER_SESSION_LIMIT_EXCEEDED) is part of named-session
    # creation. A creation refused there must not leave a reusable session behind: the
    # refused request's role-derived constraints and auth settings would otherwise be
    # frozen into a session that a later, differently authorized request then reuses.
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS one_session_profile SETTINGS max_sessions_for_user = 1"
    )
    admin("ALTER ROLE limit_role_a ADD PROFILES 'one_session_profile'")
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS limit_cap_profile SETTINGS max_threads MAX 4"
    )
    admin("ALTER ROLE limit_role_b ADD PROFILES 'limit_cap_profile'")
    session = "sess_limit"

    # Occupy the single session slot of limit_user with a long-running unnamed request.
    def blocker():
        try:
            instance.http_query(
                "SELECT sleepEachRow(1) FROM numbers(60) SETTINGS max_block_size = 1",
                user="limit_user",
                password="password_a",
                timeout=120,
            )
        except Exception:
            pass  # killed below

    thread = threading.Thread(target=blocker)
    thread.start()
    try:
        wait_condition(
            lambda: admin(
                "SELECT count() FROM system.processes WHERE user = 'limit_user'"
            ).strip(),
            lambda value: value == "1",
            max_attempts=300,
            delay=0.1,
        )
        # Creation of the named session is refused by max_sessions_for_user = 1 (from
        # limit_role_a's profile) AFTER setUser applied that role's settings/constraints and
        # the auth setting max_result_rows = 111.
        error = instance.http_query_and_get_error(
            "SELECT 1",
            user="limit_user",
            password="password_a",
            params={"session_id": session},
        )
        assert "USER_SESSION_LIMIT_EXCEEDED" in error, error
    finally:
        admin("KILL QUERY WHERE user = 'limit_user' SYNC")
        thread.join()

    # The same session_id under limit_role_b (no session limit, max_threads MAX 4,
    # max_result_rows = 555) must get a FRESH session: the constraint of the creating role
    # binds and the creating request's auth setting is applied. A leftover of the refused
    # creation would have neither (created under limit_role_a with max_result_rows = 111).
    error = instance.http_query_and_get_error(
        "SET max_threads = 16",
        user="limit_user",
        password="password_b",
        params={"session_id": session},
    )
    assert "max_threads" in error, error
    assert (
        http_sess(
            "SELECT getSetting('max_result_rows')", "limit_user", "password_b", session
        ).strip()
        == "555"
    )


def test_definer_view_rejected_for_user_with_helper_roles(started_cluster):
    # A SQL SECURITY DEFINER object created by an ephemeral user is bound to a single
    # persistent shadow `<user>:definer`, replaced on every such creation. Helper-returned
    # roles are per-authentication and may differ between simultaneous sessions of the same
    # username, so no single shadow can represent them: the creation fails closed instead
    # of persisting a definer that later creations by the same username would redefine.
    admin("GRANT CREATE VIEW ON default.* TO external_definer")
    admin("GRANT SELECT ON default.protected TO external_definer")
    error = instance.query_and_get_error(
        "CREATE VIEW default.definer_view SQL SECURITY DEFINER AS SELECT count() FROM default.protected",
        user="definer_user",
        password=GOOD_PASSWORD,
    )
    assert "SQL SECURITY DEFINER is not supported" in error, error
    assert "NOT_IMPLEMENTED" in error, error
    # Nothing was persisted: neither the view nor a shadow definer.
    assert (
        admin(
            "SELECT count() FROM system.users WHERE name = 'definer_user:definer'"
        ).strip()
        == "0"
    )
    assert admin("EXISTS TABLE default.definer_view").strip() == "0"
    # INVOKER views remain available to the same user.
    instance.query(
        "CREATE VIEW default.invoker_view SQL SECURITY INVOKER AS SELECT count() AS c FROM default.protected",
        user="definer_user",
        password=GOOD_PASSWORD,
    )
    admin("GRANT SELECT ON default.invoker_view TO external_definer")
    assert (
        instance.query(
            "SELECT c FROM default.invoker_view",
            user="definer_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "3"
    )


def test_custom_settings_follow_prefix_policy(started_cluster):
    # Returned settings follow AccessControl::checkSettingNameIsAllowed: built-in names and
    # names under custom_settings_prefixes (SQL_ here) are accepted; anything else fails the
    # authentication attempt. Custom values keep their JSON scalar type.
    assert (
        instance.query(
            "SELECT getSetting('SQL_tenant'), getSetting('SQL_region_id'), "
            "getSetting('SQL_feature_enabled'), getSetting('max_threads')",
            user="custom_settings_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "acme\t42\ttrue\t4"
    )
    # The custom setting is usable where custom settings are meant to be used: in the query.
    assert (
        instance.query(
            "SELECT count() FROM system.one WHERE getSetting('SQL_tenant') = 'acme'",
            user="custom_settings_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "1"
    )
    for user in [
        "typo_setting_user",
        "unprefixed_setting_user",
        "bad_value_setting_user",
    ]:
        error = instance.query_and_get_error(
            "SELECT 1", user=user, password=GOOD_PASSWORD
        )
        assert "Authentication failed" in error, (user, error)


def test_helper_request_counts(started_cluster):
    # Any HTTP response the helper returns is final: it is parsed exactly once and never
    # re-sent to the helper, so a rejected password, a rate limit, a server error or a
    # malformed body each cost the helper exactly one request. Only transport failures
    # (no HTTP response at all) are retried, up to max_tries (3 in config_main.xml).
    for user, password in [
        ("http_user", "wrong_password"),
        ("err429_user", GOOD_PASSWORD),
        ("err500_user", GOOD_PASSWORD),
        ("malformed_json_user", GOOD_PASSWORD),
        ("bad_roles_type_user", GOOD_PASSWORD),
    ]:
        before = helper_request_count(instance, user)
        error = instance.query_and_get_error("SELECT 1", user=user, password=password)
        assert error, (user, error)
        assert (
            helper_request_count(instance, user) - before == 1
        ), f"a parsed HTTP response must not be retried for user {user}"

    # The 401 case matches the wording other tests in this file assert on.
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="http_user", password="wrong_password"
    )

    # A transport failure (connection closed without any HTTP response) is retried up to
    # max_tries.
    before = helper_request_count(instance, "conn_reset_user")
    error = instance.query_and_get_error(
        "SELECT 1", user="conn_reset_user", password=GOOD_PASSWORD
    )
    assert error
    assert (
        helper_request_count(instance, "conn_reset_user") - before == 3
    ), "a transport failure must be retried exactly max_tries times"


def test_distributed_query_does_not_rebuild_profiles_from_propagated_roles(
    started_cluster,
):
    # Regression for the provenance rule. An ordinary user gets a NON-default role whose
    # profile caps max_threads at 4 and sets max_rows_to_read_leaf = 1. The session is
    # created without the role, so without that profile; `SET ROLE` inside the session
    # enables the role for authorization but does not rebuild the session's creation-time
    # profile state, so `SET max_threads = 16` succeeds. A Distributed query then pushes the
    # current role to node2 for authorization only. If node2 treated the propagated role as
    # fresh authentication-time profile input, it would install the role's profile there:
    # max_rows_to_read_leaf = 1 (a value the initiator never sends, because it is unchanged
    # on the initiator) is enforced on node2's own read of two rows and fails the query.
    # (A MAX constraint alone cannot detect this: a secondary query's propagated settings are
    # clamped quietly, not rejected.)
    for run in [admin, admin2]:
        run("CREATE USER IF NOT EXISTS replay_user IDENTIFIED BY 'replay_password'")
        run("CREATE ROLE IF NOT EXISTS replay_constraint_role")
        run(
            "CREATE SETTINGS PROFILE IF NOT EXISTS replay_cap_profile "
            "SETTINGS max_threads MAX 4, max_rows_to_read_leaf = 1"
        )
        run("ALTER ROLE replay_constraint_role ADD PROFILES 'replay_cap_profile'")
        run("GRANT SELECT ON default.* TO replay_constraint_role")
        run("GRANT replay_constraint_role TO replay_user")
        run("ALTER USER replay_user DEFAULT ROLE NONE")
        run(
            "CREATE TABLE IF NOT EXISTS default.replay_local (x UInt64) ENGINE = MergeTree ORDER BY x"
        )
    admin2("INSERT INTO default.replay_local VALUES (7), (8)")
    admin(
        "CREATE TABLE IF NOT EXISTS default.replay_distributed AS default.replay_local "
        "ENGINE = Distributed(test_cluster, default, replay_local)"
    )
    session = "sess_replay_constraint"

    def in_session(sql):
        return instance.http_query(
            sql,
            user="replay_user",
            password="replay_password",
            params={"session_id": session},
        )

    in_session("SELECT 1")  # creates the session without the role's constraint
    in_session("SET ROLE replay_constraint_role")
    in_session(
        "SET max_threads = 16"
    )  # must succeed: SET ROLE does not rebuild constraints
    assert in_session("SELECT getSetting('max_threads')").strip() == "16"
    # sum(x), not count(): a trivial count is answered from part metadata without
    # reading rows, so the leaf read limit would never be consulted.
    assert in_session("SELECT sum(x) FROM default.replay_distributed").strip() == "15"


def test_response_framing_contract(started_cluster):
    # A 200 body carries authentication state, so its complete reception must be verifiable:
    # Content-Length or chunked framing is required, and a detectable truncation fails the
    # attempt. A body delimited only by connection close is rejected even when it is valid,
    # because a helper dying right after the headers would be indistinguishable from an
    # empty response. None of these failures is retried (one helper request each).
    for user in [
        "truncated_200_user",
        "close_delimited_empty_user",
        "close_delimited_body_user",
    ]:
        before = helper_request_count(instance, user)
        error = instance.query_and_get_error(
            "SELECT 1", user=user, password=GOOD_PASSWORD
        )
        assert "Authentication failed" in error, (user, error)
        assert helper_request_count(instance, user) - before == 1, user
    # Content-Length: 0 keeps the "empty 200 means {}" contract: the rejection above is about
    # unverifiable framing, not about empty bodies.
    assert (
        instance.query(
            "SELECT currentUser()",
            user="content_length_zero_user",
            password=GOOD_PASSWORD,
        ).strip()
        == "content_length_zero_user"
    )
    # A truncated 404 (Content-Length: 100, connection closed after 2 bytes) is an incomplete
    # response, not a "user not found": on node3, where the http directory precedes users.xml
    # and truncated_404_user exists in users.xml with this password, the attempt must fail
    # closed rather than fall through to the local user.
    before = helper_request_count(instance3, "truncated_404_user")
    error = instance3.query_and_get_error(
        "SELECT 1", user="truncated_404_user", password="local_pw"
    )
    assert "Authentication failed" in error, error
    assert helper_request_count(instance3, "truncated_404_user") - before == 1
