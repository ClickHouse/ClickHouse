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


def admin3(query, **kwargs):
    return instance3.query(
        query, user="admin_user", password="admin_password", **kwargs
    )


def query4_local(query, user=None, password=None, nothrow=False):
    # node4's directory restricts `networks` to `127.0.0.1/32`; `instance4.query` runs
    # `clickhouse-client` on the test-runner host over the docker bridge network, which node4
    # never sees as `127.0.0.1`. Run the client inside node4's own container instead, against
    # its own loopback. stderr is redirected into stdout because `exec_in_container` with
    # `nothrow=True` only returns stdout.
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


# --- Happy path and roles ---


def test_http_directory_registered_on_every_node(started_cluster):
    for node in [instance, instance2, instance3, instance4]:
        directories = node.query(
            "SELECT count() FROM system.user_directories WHERE type = 'http'",
            user="admin_user",
            password="admin_password",
        ).strip()
        assert directories == "1"


def test_node3_directory_order_http_before_users_xml(started_cluster):
    # node3's http directory must precede users.xml, with no implicit users_config ahead of
    # it; system.user_directories orders storages by precedence.
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
    # Two authentications of the same username with different helper-returned role sets each
    # get exactly their own set, and the cached user never accumulates grants.
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
    # Reattaching a named session under a different authentication rebinds its roles at
    # request time; this is a sequential rebind check, not proof of concurrent isolation
    # between two sessions held open at once.
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
    # The session-scoped model rests on the storage being read-only: nobody can attach
    # persistent grants or alter the ephemeral user.
    instance.query(
        "SELECT 1", user="http_user", password=GOOD_PASSWORD
    )  # ensure materialized
    for ddl in [
        "GRANT reader TO http_user",
        "GRANT SELECT ON default.protected TO http_user",
        # DEFAULT ROLE NONE (not a named role) reaches the storage's readonly write path
        # directly, instead of failing earlier in
        # InterpreterSetRoleQuery::updateUserSetDefaultRoles's check that the role is
        # already granted to the user.
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
    # An empty password must fail like any wrong password, fail-closed.
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="http_user", password=""
    )


def test_role_dropped_and_recreated(started_cluster):
    # Roles are resolved by name at every authentication, not by a cached UUID: while the
    # role is dropped, authentication fails closed (unlike LDAP, which skips the role); after
    # recreation, authentication succeeds with the new role entity.
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


# --- Fallback and failure matrix ---


def test_404_falls_through_to_later_storage(started_cluster):
    # node3 orders the http directory before users.xml; admin_user is unknown to the helper
    # (404), so authentication must fall through to users.xml and succeed. Use a fresh admin
    # credential name so the mock-consulted assertion below is unambiguous.
    assert (
        instance3.query(
            "SELECT currentUser()", user="admin_user", password="admin_password"
        ).strip()
        == "admin_user"
    )
    # Confirms the http directory was actually consulted first (the mock saw admin_user)
    # rather than skipped.
    seen = instance3.exec_in_container(
        ["bash", "-c", "curl -s 'http://localhost:8000/seen?user=admin_user'"]
    ).strip()
    assert seen == "1", "http directory was not consulted before users.xml"


def test_wrong_password_does_not_fall_through(started_cluster):
    # shadowed_user exists in both the helper (http directory, first) and users.xml (second,
    # password xml_password). The helper rejects xml_password with 401, which must fail
    # closed without falling through to users.xml.
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


# --- Settings ---


def test_default_profile_resolved_late_and_fail_closed(started_cluster):
    admin4 = lambda q: instance4.query(q, user="admin_user", password="admin_password")
    # node4's directory declares default_profile=sql_profile, which does not exist yet: the
    # very first materialization must fail closed.
    assert "Authentication failed" in query4_local(
        "SELECT 1", user="aux_user", password=GOOD_PASSWORD, nothrow=True
    )
    # After the profile is created, materialization succeeds and the profile applies.
    admin4(
        "CREATE SETTINGS PROFILE IF NOT EXISTS sql_profile SETTINGS max_rows_to_read = 12345"
    )
    value = query4_local(
        "SELECT getSetting('max_rows_to_read')", user="aux_user", password=GOOD_PASSWORD
    ).strip()
    assert value == "12345"


def test_networks_allow_localhost(started_cluster):
    # Must use query4_local: instance4.query connects over the docker bridge network, which
    # node4's `127.0.0.1/32` networks policy never allows. Rejection from a remote client is
    # covered by test_networks_reject_remote_client below.
    assert (
        query4_local("SELECT 1", user="aux_user", password=GOOD_PASSWORD).strip() == "1"
    )


def test_response_settings_override_profile_value(started_cluster):
    # Response settings apply after profile initialization and override the profile value
    # (sql_profile sets max_rows_to_read=12345, the response returns 777). Recreate
    # sql_profile with IF NOT EXISTS so this test doesn't depend on run order.
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
    # Infrastructure failure is fail-closed (never a fallthrough). Kill node4's mock server,
    # authenticate, then restart the mock for later tests.
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
    # getOrCreateUser re-resolves default_profile on every authentication, not just at first
    # materialization, and fails closed for an already-cached user too. Sequence: authenticate
    # (cached) -> drop profile -> authenticate fails closed -> recreate the same name with a
    # different value -> authenticate succeeds with the new value.
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
    # This is the last test in this file that depends on sql_profile's specific value; later
    # tests only rely on sql_profile's existence, not its value.


def test_response_settings_apply(started_cluster):
    value = instance.query(
        "SELECT getSetting('max_threads')",
        user="profileclash_user",
        password=GOOD_PASSWORD,
    ).strip()
    assert value == "7"


# --- Fallback and failure matrix ---


def test_local_user_shadows_helper_user(started_cluster):
    # On node (users.xml first), local_user exists in users.xml AND in the helper. The xml
    # password works; the helper password does not, because users.xml finds the user first
    # and fails closed on a wrong password without falling through.
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


# --- Cache bound and concurrency ---


def test_max_cached_users_bound(started_cluster):
    # node3 allows at most 3 materialized users (soft bound; the test is sequential, so no
    # overshoot occurs). Count already-materialized users first (shadowed_user may already
    # hold a slot) and fill up to the bound deterministically.
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
    # Remote HTTP I/O is not serialized by a directory-wide lock: N distinct usernames
    # authenticate at once, and the mock's handler barriers all N requests before releasing
    # any. A directory-wide lock would keep the barrier from ever filling (the second request
    # never reaches the mock while the first is blocked), so requests would time out instead.
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


# --- Distributed ---


def test_distributed_query_propagates_helper_roles(started_cluster):
    # Role grants and the local table exist on both nodes; data lives only on node2, and the
    # Distributed table sits on the initiator.
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
    # distributed_user gets cluster_role from the helper on node; the role must be effective
    # on node2 via interserver propagation, where the user is materialized through
    # AlwaysAllowCredentials without an HTTP request.
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
    # On the AlwaysAllowCredentials path, the receiving node must not call its own HTTP auth
    # server: stop node2's mock, then run a distributed query as a fresh (uncached-on-node2)
    # helper user. node2 must still materialize the user and the query must still succeed.
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
    # Same distributed read as above, but through a normal VIEW on the initiator.
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
    # halfcluster_user gets [reader, only_node1_role]; only_node1_role exists only on `node`.
    # Propagated roles travel via ClientInfo.current_roles, and the receiver fails closed
    # (ACCESS_DENIED) on any unresolvable role rather than dropping it and running with the
    # resolvable remainder.
    admin("GRANT SELECT ON default.* TO only_node1_role")
    error = instance.query_and_get_error(
        "SELECT count() FROM default.distributed_table",
        user="halfcluster_user",
        password=GOOD_PASSWORD,
    )
    assert "current roles" in error or "ACCESS_DENIED" in error


def test_networks_reject_remote_client(started_cluster):
    # node4's directory allows only 127.0.0.1/32. A client connecting from node2's address
    # must be rejected (the fail-closed networks row of the matrix; complements the allow
    # test above). This image has no `clickhouse-client` binary; use `/usr/bin/clickhouse
    # client` instead.
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


# --- Named sessions ---


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
    # A pre-created user with IDENTIFIED WITH HTTP also gets its auth-server settings at
    # named-session creation.
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
    # Role-derived settings and constraints are established when the named session is created
    # and are not rebuilt when a later reattachment rebinds the role set; privileges and row
    # policies do follow the rebind (see test_named_session_rebind_replaces_roles).
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
    # Direction 2: a session CREATED under probe_role_a (no constraint) keeps its looser
    # settings state after a rebind to probe_role_b — documented behavior.
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
    # Two native (persistent TCP) connections authenticate once each with different absolute
    # deadlines and stay open concurrently. Per-query expiry enforcement
    # (Session::checkIfUserIsStillValid) then applies to each independently, so the short one
    # expires while the long one keeps working. Both clients start at the same time and each
    # sends statements spaced by client-side sleeps so their last statements run after the
    # short deadline.
    # This image has no `clickhouse-client` binary; use `/usr/bin/clickhouse client` instead.
    # `--multiquery` also errors on this version (multi-statement queries are the default
    # now), so it is dropped.
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
    # A plain reattach can't prove named-session expiry replacement, because every HTTP
    # request re-authenticates and so already carries its own deadline. An async insert is
    # genuinely deferred: it captures the authentication deadline at enqueue and re-checks it
    # at flush, throwing USER_EXPIRED if the deadline has passed. Create the session under a
    # short deadline, reattach under a long one (which must overwrite the stored expiry), then
    # let the flush land after the short deadline: the row lands only if the reattach's
    # deadline was actually honored.
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
    # 5s short deadline. async_insert_use_adaptive_busy_timeout defaults to true, which makes
    # the flush delay ramp from async_insert_busy_timeout_min_ms toward the max rather than
    # use max_ms directly — nondeterministic, and possibly flushing before the short deadline.
    # Disable it so async_insert_busy_timeout_max_ms applies directly.
    # http_query raises on any server error, so a missing setter (flush-time USER_EXPIRED)
    # fails this test with the actual server error; http_query_and_get_error would be wrong
    # here since it raises when the query succeeds. This request deliberately takes ~7 seconds.
    started = time.monotonic()
    instance.http_query(
        "INSERT INTO default.async_target SETTINGS async_insert=1, wait_for_async_insert=1, "
        "async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_max_ms=7000 VALUES (1)",
        user="expiry_user",
        password=f"until_{now + 3600}",
        params={"session_id": session},
        # http_query defaults to GET unless `data` is passed, and INSERT over GET is rejected
        # as readonly; force POST explicitly.
        method="POST",
        timeout=20,
    )
    elapsed = time.monotonic() - started
    # The request must have genuinely waited past the short (5s) deadline for the flush-time
    # expiry re-check to be what's under test, rather than an already-fast flush merely
    # passing.
    assert elapsed > 5, (
        f"async insert returned in {elapsed:.1f}s, too fast to have exercised "
        "the flush-time expiry re-check"
    )
    assert instance.query("SELECT count() FROM default.async_target").strip() == "1"


def test_failed_named_session_init_not_reusable(started_cluster):
    # Creating a named session can fail after acquireSession has already published it
    # (checkSettingsConstraints throws when a returned setting violates the returned role's
    # profile). The failed session must not remain reusable: a later request with the same
    # session_id must get a fresh session that applies its own auth settings, not the
    # half-initialized one.
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
    # Request 2: same session_id, a valid authentication returning max_result_rows=555. If the
    # failed session were left reusable, this would take the reuse branch and not apply auth
    # settings, so max_result_rows would not be 555.
    value = instance.http_query(
        "SELECT getSetting('max_result_rows')",
        user="guard_user",
        password="valid",
        params={"session_id": session},
    ).strip()
    assert value == "555"


def _event_value(event_name):
    # system.events is cumulative since server startup and has no row for an event that has
    # never fired, so a missing row is treated as 0.
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
    # startup, so an absolute `> 0` assertion would pass regardless of what this test does.
    # Nothing else runs concurrently in this suite, so a snapshot-then-act-then-snapshot pair
    # is race-free.
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

    # 4. A username the mock server does not know: 404 (UserNotFound) falls through, then
    # fails overall because users.xml precedes http (the last storage on `node`), so
    # throw_if_user_not_exists is true for it. Not a directory failure:
    # HTTPUserDirectoryAuthFailures must stay unchanged.
    before = snapshot()
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="metrics_ghost_user", password=GOOD_PASSWORD
    )
    after = snapshot()
    assert after["requests"] == before["requests"] + 1
    assert after["failures"] == before["failures"]


# --- Named sessions ---


def test_named_session_refused_by_session_limit_not_reusable(started_cluster):
    # Admission (trackSession, USER_SESSION_LIMIT_EXCEEDED) is part of named-session creation.
    # A creation refused there must not leave a reusable session behind: the refused request's
    # role-derived constraints and auth settings would otherwise be frozen into a session that
    # a later, differently authorized request then reuses.
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
        # limit_role_a's profile) after setUser applied that role's settings/constraints and
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
    # max_result_rows = 555) must get a fresh session: the creating role's constraint binds
    # and the creating request's auth setting is applied. A leftover of the refused creation
    # would have neither.
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
    # username, so no single shadow can represent them: the creation fails closed instead of
    # persisting a definer that a later creation by the same username would redefine.
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


# --- Framing and retries ---


def test_helper_request_counts(started_cluster):
    # Any HTTP response the helper returns is final: it is parsed exactly once and never
    # re-sent to the helper, so a rejected password, a rate limit, a server error or a
    # malformed body each cost the helper exactly one request. Only transport failures (no
    # HTTP response at all) are retried, up to max_tries (3 in config_main.xml).
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
    # A role's profile (max_threads MAX 4, max_rows_to_read_leaf = 1) is granted but not part
    # of the session's creation-time state; SET ROLE enables it for authorization without
    # rebuilding that state, so SET max_threads = 16 succeeds locally.
    # The Distributed query then pushes the role to node2 for authorization only: if node2
    # treated it as fresh profile input, it would enforce max_rows_to_read_leaf = 1 there and
    # fail the read of two rows — a MAX constraint alone wouldn't catch this, since propagated
    # settings are clamped quietly rather than rejected.
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
    # sum(x), not count(): a trivial count is answered from part metadata without reading
    # rows, so the leaf read limit would never be consulted.
    assert in_session("SELECT sum(x) FROM default.replay_distributed").strip() == "15"


def test_response_framing_contract(started_cluster):
    # A 200 body carries authentication state, so its complete reception must be verifiable:
    # Content-Length or chunked framing is required, and a detectable truncation fails the
    # attempt. A body delimited only by connection close is rejected even when it is valid,
    # because a helper dying right after the headers would be indistinguishable from an empty
    # response. None of these failures is retried (one helper request each).
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


# --- Reload ---


def test_reload_users_drops_materialized_users(started_cluster):
    # SYSTEM RELOAD USERS drops every materialized user of the directory. That makes the
    # max_cached_users bound recoverable without a restart and releases a name that a
    # materialized user held from CREATE USER (node3: <http> precedes the writable
    # local_directory, so the held name selects the read-only http storage for insertion).
    # Fill node3's bound (3) if it isn't already full.
    already = int(
        admin3("SELECT count() FROM system.users WHERE storage = 'http'").strip()
    )
    for i in range(3 - already):
        instance3.query("SELECT 1", user=f"cache_user_{i}", password=GOOD_PASSWORD)
    assert "Authentication failed" in instance3.query_and_get_error(
        "SELECT 1", user="cache_user_8", password=GOOD_PASSWORD
    )
    held = admin3(
        "SELECT name FROM system.users WHERE storage = 'http' LIMIT 1"
    ).strip()
    assert (
        "readonly"
        in instance3.query_and_get_error(
            f"CREATE USER {held} IDENTIFIED BY 'local_pw'",
            user="admin_user",
            password="admin_password",
        ).lower()
    )

    admin3("SYSTEM RELOAD USERS")

    assert (
        admin3("SELECT count() FROM system.users WHERE storage = 'http'").strip() == "0"
    )
    assert (
        admin3(
            "SELECT value FROM system.metrics WHERE metric = 'HTTPUserDirectoryCachedUsers'"
        ).strip()
        == "0"
    )
    # The bound is recoverable: a previously rejected new username authenticates now.
    assert (
        instance3.query("SELECT 1", user="cache_user_8", password=GOOD_PASSWORD).strip()
        == "1"
    )
    # The released name can be created locally.
    admin3(f"CREATE USER {held} IDENTIFIED BY 'local_pw'")
    assert (
        admin3(f"SELECT storage FROM system.users WHERE name = '{held}'").strip()
        == "local_directory"
    )
    admin3(f"DROP USER {held}")


def test_error_response_with_body_does_not_poison_connection(started_cluster):
    # A 401 body is never read; the pooled connection must be discarded, not reused with
    # unread bytes, so the next authentication over the same helper works. Each response is
    # final: exactly one helper request per attempt.
    for _ in range(2):
        before = helper_request_count(instance, "body401_user")
        assert "Authentication failed" in instance.query_and_get_error(
            "SELECT 1", user="body401_user", password="wrong_password"
        )
        assert helper_request_count(instance, "body401_user") - before == 1
        before = helper_request_count(instance, "body401_user")
        assert (
            instance.query(
                "SELECT currentUser()", user="body401_user", password=GOOD_PASSWORD
            ).strip()
            == "body401_user"
        )
        assert helper_request_count(instance, "body401_user") - before == 1


def test_duplicate_json_members_rejected(started_cluster):
    assert "Authentication failed" in instance.query_and_get_error(
        "SELECT 1", user="dup_roles_user", password=GOOD_PASSWORD
    )


def test_refused_reattachment_leaves_session_intact(started_cluster):
    # A reattachment refused by admission (USER_SESSION_LIMIT_EXCEEDED from the reattaching
    # role's profile) must leave the named session as it was: its SET state survives and the
    # next reattachment binds its own roles.
    admin(
        "CREATE SETTINGS PROFILE IF NOT EXISTS one_session_profile SETTINGS max_sessions_for_user = 1"
    )
    admin("ALTER ROLE limit_role_a ADD PROFILES 'one_session_profile'")
    session = "sess_refused_reattach"
    in_session = lambda sql, password: instance.http_query(
        sql, user="limit_user", password=password, params={"session_id": session}
    )
    in_session(
        "SELECT 1", "password_b"
    )  # created under limit_role_b (no session limit)
    in_session("SET max_block_size = 777", "password_b")

    def blocker():
        try:
            instance.http_query(
                "SELECT sleepEachRow(1) FROM numbers(60) SETTINGS max_block_size = 1",
                user="limit_user",
                password="password_b",
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
        # Reattaching under limit_role_a (max_sessions_for_user = 1) is refused: the slot is
        # taken by the blocker.
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

    assert (
        in_session("SELECT getSetting('max_block_size')", "password_b").strip() == "777"
    )
    assert (
        in_session("SELECT arrayJoin(currentRoles())", "password_b").strip()
        == "limit_role_b"
    )
