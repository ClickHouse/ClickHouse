"""
Regression test for https://github.com/ClickHouse/ClickHouse/issues/99574 .

`EXECUTE AS <ldap_user>` must work for users authenticated through an external LDAP
user directory even when the target user has not yet logged in since the most recent
server restart. The LDAP user-directory only populates its in-memory user entry on
first successful authentication, so before the fix the AccessControl name lookup
returned `UNKNOWN_USER` and impersonation failed with:

    Code: 192. DB::Exception: There is no user `janedoe` in `user directories`.

The fix exposes `IAccessStorage::find(..., force_external_lookup=true)`, which
`LDAPAccessStorage` implements by service-binding against the configured LDAP server
(see `lookup_bind_dn` / `lookup_password` in `configs/ldap.xml`) and resolving the
user via `user_dn_detection` before materializing the in-memory entry with the
resolved role mapping.

The tests on `node` rely on test execution order: `cluster.start` runs once for the
module, leaving the LDAP storage's in-memory cache empty, so the first test exercises
the not-yet-logged-in path before any subsequent test materializes those users. The
tests on `node_with_role_mapping`, `node_bad_lookup_password`,
`node_with_local_user_precedence`, `node_misconfigured_lookup`,
`node_with_role_mapping_user_dn`, `node_static_user_dn_detection`, `shard1_node`,
`shard2_node`, and `node_with_two_ldap_directories` exercise the role-mapping,
misconfiguration, local-vs-LDAP precedence, surfaced-config-error, AD-style
`{user_dn}` mapping, target-independent `user_dn_detection`,
distributed-cache-poisoning, and two-LDAP-directory cache-refresh variants on
instances that no other test in this module logs into outside of its dedicated
test, so each instance's LDAP cache state is controlled by exactly one test.
"""

import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/ldap.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_ldap=True,
)

# Instance with `role_mapping` configured under the LDAP user-directory. Used to
# verify that the forced-lookup path applies role mappings before first login.
node_with_role_mapping = cluster.add_instance(
    "node_with_role_mapping",
    main_configs=["configs/ldap_with_role_mapping.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Instance whose `lookup_password` is deliberately wrong (but non-empty). Used to
# verify that an invalid service-bind credential surfaces as an LDAP configuration
# error instead of silently falling through to `UNKNOWN_USER`.
node_bad_lookup_password = cluster.add_instance(
    "node_bad_lookup_password",
    main_configs=["configs/ldap_wrong_lookup_password.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Instance with both an LDAP user-directory (ordered first) and a local user-xml
# directory that defines a `janedoe` user. Used to verify that `EXECUTE AS
# janedoe` resolves to the LOCAL user instead of materializing the LDAP one,
# preserving the resolution precedence the pre-PR `getID<User>` path had.
node_with_local_user_precedence = cluster.add_instance(
    "node_with_local_user_precedence",
    main_configs=["configs/ldap_with_local_user_precedence.xml"],
    user_configs=["configs/users_with_local_janedoe.xml"],
    stay_alive=True,
)

# Instance whose LDAP server config sets `lookup_bind_dn` and `lookup_password`
# but omits `user_dn_detection`. `parseLDAPServer` rejects this shape and the
# server is dropped from the blueprint. Used to verify that the saved parse
# error surfaces as a query-time exception rather than degrading to
# `UNKNOWN_USER`.
node_misconfigured_lookup = cluster.add_instance(
    "node_misconfigured_lookup",
    main_configs=["configs/ldap_misconfigured_lookup.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Instance whose `role_mapping` filter uses `{user_dn}` (the AD-style
# substitution) rather than `{bind_dn}`. The forced-lookup path runs
# `user_dn_detection` before the role search, so `{user_dn}` should resolve to
# the DN the search returned. Verifies the AD-style mapping path through the
# service-bind lookup.
node_with_role_mapping_user_dn = cluster.add_instance(
    "node_with_role_mapping_user_dn",
    main_configs=["configs/ldap_with_role_mapping_user_dn.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Instance whose `user_dn_detection` is target-independent: a static `search_filter`
# (`(cn=janedoe)`) and a static `base_dn`, with `lookup_bind_dn` configured. The
# search would always return the same single entry, so without target-specificity
# `EXECUTE AS not_in_ldap` would resolve `final_user_dn` from the matching entry
# and materialize a phantom user. `parseLDAPServer` rejects this shape, the saved
# parse error surfaces at query time.
node_static_user_dn_detection = cluster.add_instance(
    "node_static_user_dn_detection",
    main_configs=["configs/ldap_static_user_dn_detection.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Two-shard cluster instances configured with `<secret>foo</secret>`, so the
# interserver protocol authenticates remote queries via
# `AlwaysAllowCredentials{initial_user}`. Used to reproduce the distributed
# `EXECUTE AS` cache-poisoning scenario: a fanout under `<secret>` materializes
# the target user on the receiving shard with empty `users_external_roles` (no
# role mapping resolved). A subsequent local `EXECUTE AS <same user>` on that
# shard must still pick up the role mapping, which the fix arranges by asking
# the LDAP storage to refresh entries whose `users_external_roles` is incomplete.
shard1_node = cluster.add_instance(
    "shard1_node",
    main_configs=["configs/ldap_cluster_with_secret.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

shard2_node = cluster.add_instance(
    "shard2_node",
    main_configs=["configs/ldap_cluster_with_secret.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# Instance with TWO LDAP user-directories backed by the same OpenLDAP container,
# each with a distinct role-mapping prefix so the directory that resolves a user
# can be identified by the role granted on materialization. Used to verify that
# the cache-refresh layer in `InterpreterExecuteAsQuery::resolveImpersonationTargetUser`
# refreshes only the storage that won pass 1 rather than starting a global
# `force_external_lookup` that could materialize the same user in an earlier
# storage and change which directory wins.
node_with_two_ldap_directories = cluster.add_instance(
    "node_with_two_ldap_directories",
    main_configs=["configs/ldap_two_directories.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def add_ldap_group(ldap_cluster, group_cn, member_cn):
    """Add a `groupOfNames` entry to the LDAP fixture with the given group CN and a
    single member referenced by their user CN under `ou=users,dc=example,dc=org`.
    Mirrors the helper used by `test_ldap_external_user_directory`.
    """
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """echo "dn: cn={group_cn},dc=example,dc=org
objectClass: top
objectClass: groupOfNames
member: cn={member_cn},ou=users,dc=example,dc=org" | \
ldapadd -H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
    """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
                member_cn=member_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_execute_as add_ldap_group code:{code} stdout:{stdout} stderr:{stderr}"
    )
    assert code == 0


def delete_ldap_group(ldap_cluster, group_cn):
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """ldapdelete -H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password} "cn={group_cn},dc=example,dc=org"
    """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                group_cn=group_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_execute_as delete_ldap_group code:{code} stdout:{stdout} stderr:{stderr}"
    )
    assert code == 0


def test_execute_as_ldap_user_without_prior_login(started_cluster):
    """The reproducer from the issue.

    `janedoe` is provisioned in the openldap fixture (see
    tests/integration/compose/docker_compose_ldap.yml). We deliberately do NOT call
    any query that would authenticate `janedoe` first, so that the LDAP storage's
    in-memory cache for that user is empty when `EXECUTE AS janedoe` is invoked.

    Before the fix this raises `UNKNOWN_USER`. After the fix the EXECUTE AS resolver
    asks the LDAP storage for `force_external_lookup`, which service-binds against
    the directory, confirms the user via `user_dn_detection`, and materializes the
    in-memory entry. Impersonation then succeeds.
    """
    result = node.query(
        "EXECUTE AS janedoe SELECT currentUser()",
        user="admin",
        password="qwerty",
    )
    assert result.strip() == "janedoe"


def test_execute_as_ldap_user_after_login_still_works(started_cluster):
    """Sanity check: the previously-working path (target user already logged in) is
    not regressed by the fix.
    """
    # First log `johndoe` in so they are materialized in the LDAP storage cache.
    assert node.query(
        "SELECT currentUser()", user="johndoe", password="qwertz"
    ).strip() == "johndoe"

    # Now impersonate as `admin`. This already worked before the fix.
    result = node.query(
        "EXECUTE AS johndoe SELECT currentUser()",
        user="admin",
        password="qwerty",
    )
    assert result.strip() == "johndoe"


def test_execute_as_requires_impersonate_grant(started_cluster):
    """Security check: the forced lookup must not bypass the IMPERSONATE access check.

    `restricted` is a users.xml user without `access_management`, so it has no
    IMPERSONATE grant on any user. EXECUTE AS must fail with an access denied error
    before the LDAP forced-lookup path has a chance to materialize the target user.
    """
    error = node.query_and_get_error(
        "EXECUTE AS janedoe SELECT 1",
        user="restricted",
        password="qwerty",
    )
    assert "ACCESS_DENIED" in error or "Not enough privileges" in error


def test_execute_as_unknown_ldap_user_throws(started_cluster):
    """Security check: a name that does not exist in LDAP must produce
    `UNKNOWN_USER`, not silently materialize a phantom user. This is the property
    the previous `AlwaysAllowCredentials` fallback violated: it created cache
    entries for any name. The forced-find path verifies existence via
    `user_dn_detection` before materializing.
    """
    error = node.query_and_get_error(
        "EXECUTE AS nonexistent_ldap_user SELECT 1",
        user="admin",
        password="qwerty",
    )
    assert "UNKNOWN_USER" in error or "no user" in error


def test_execute_as_ldap_user_with_role_mapping_without_prior_login(started_cluster):
    """Verifies that the forced-lookup path applies `role_mapping` from the LDAP
    user-directory configuration. Without this, the EXECUTE AS target would be
    materialized with only the common roles instead of the privileges it would gain
    from a real LDAP login (the property the bot's review on
    `tests/integration/test_ldap_execute_as/test.py` asked us to cover).

    Strategy:
      1. Create a local role on the impersonator instance with a non-trivial
         privilege (read on a specific table) and revoke that privilege at user
         scope so only the role can grant it.
      2. Add a `clickhouse-pre_login_role` group to LDAP with `janedoe` as a
         member. The `role_mapping` strips the `clickhouse-` prefix, so the LDAP
         group maps to the local `pre_login_role` role.
      3. Run `EXECUTE AS janedoe` from `admin` on `node_with_role_mapping`, where
         `janedoe` has never authenticated. The forced lookup must materialize
         `janedoe` with `pre_login_role` granted via `role_mapping`.
      4. Confirm the impersonated session reports `pre_login_role` as a current
         role; the role grant is the only thing that makes that visible without a
         login.
    """
    node_with_role_mapping.query(
        "CREATE ROLE IF NOT EXISTS pre_login_role",
        user="admin",
        password="qwerty",
    )
    try:
        add_ldap_group(
            started_cluster,
            group_cn="clickhouse-pre_login_role",
            member_cn="janedoe",
        )

        # `janedoe` has never authenticated on `node_with_role_mapping`. Before the
        # fix, `AccessControl::find<User>("janedoe")` returned nothing and EXECUTE AS
        # raised `UNKNOWN_USER`. After the fix, the forced-lookup path materializes
        # `janedoe` with `pre_login_role` granted via `role_mapping`.
        current_roles = node_with_role_mapping.query(
            "EXECUTE AS janedoe SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="admin",
            password="qwerty",
        )
        assert current_roles.strip() == "pre_login_role", current_roles
    finally:
        delete_ldap_group(started_cluster, group_cn="clickhouse-pre_login_role")
        node_with_role_mapping.query(
            "DROP ROLE IF EXISTS pre_login_role",
            user="admin",
            password="qwerty",
        )


def test_execute_as_local_user_takes_precedence_over_ldap(started_cluster):
    """Bot concern on `src/Interpreters/Access/InterpreterExecuteAsQuery.cpp`:
    the forced LDAP lookup must not be allowed to shadow a target name that
    already exists in a later access storage. Without a two-pass lookup,
    `find<User>(name, force_external_lookup=true)` would let the first storage
    (`LDAPAccessStorage`, ordered before `users_xml` on
    `node_with_local_user_precedence`) materialize the LDAP `janedoe` even
    though a local `janedoe` already exists in `users.xml`. The impersonated
    session would then run with LDAP-resolved roles instead of the local user's
    roles.

    Strategy:
      1. `EXECUTE AS janedoe` from `admin`. Both the LDAP and local `janedoe`
         exist; the local one must win because the first (non-forced)
         `find<User>` returns it from `users_xml`.
      2. Assert that the LDAP storage was NOT consulted by checking that no
         `janedoe` entry was materialized in the `ldap` storage. Before the
         fix, the forced lookup would have created an entry there. After the
         fix, the forced lookup is bypassed entirely because the first pass
         already returned the local user, and the LDAP storage stays empty.
    """
    # Sanity-check the setup: local `janedoe` exists in `users_xml`, the LDAP
    # storage exists but has no `janedoe` entry yet on this instance.
    initial_local_count = node_with_local_user_precedence.query(
        "SELECT count() FROM system.users "
        "WHERE name = 'janedoe' AND storage = 'users_xml'",
        user="admin",
        password="qwerty",
    )
    assert initial_local_count.strip() == "1", initial_local_count
    initial_ldap_count = node_with_local_user_precedence.query(
        "SELECT count() FROM system.users "
        "WHERE name = 'janedoe' AND storage = 'ldap'",
        user="admin",
        password="qwerty",
    )
    assert initial_ldap_count.strip() == "0", initial_ldap_count

    # The actual precedence check: EXECUTE AS must resolve to the local user.
    result = node_with_local_user_precedence.query(
        "EXECUTE AS janedoe SELECT currentUser()",
        user="admin",
        password="qwerty",
    )
    assert result.strip() == "janedoe"

    # After the fix, the forced LDAP lookup is not invoked because the first
    # pass already returned the local user. The LDAP storage's in-memory cache
    # must therefore still have no `janedoe` entry. Before the fix this count
    # was `1` (the forced lookup materialized the LDAP user).
    post_ldap_count = node_with_local_user_precedence.query(
        "SELECT count() FROM system.users "
        "WHERE name = 'janedoe' AND storage = 'ldap'",
        user="admin",
        password="qwerty",
    )
    assert post_ldap_count.strip() == "0", (
        "EXECUTE AS materialized the LDAP `janedoe` even though a local "
        "`janedoe` exists: got " + post_ldap_count
    )


def test_execute_as_misconfigured_lookup_throws_config_error(started_cluster):
    """`@tiandiwonder` blocker on `src/Access/ExternalAuthenticators.cpp`: when
    `parseLDAPServer` rejected a server (e.g. `lookup_bind_dn` configured but
    `user_dn_detection` missing), `setConfiguration` caught the exception, logged
    a `Could not parse LDAP server` line, and silently dropped the server from
    `ldap_client_params_blueprint`. `findLDAPUser` then returned `false` for any
    name on that directory and `EXECUTE AS` collapsed to `UNKNOWN_USER` (or
    `there is no user`), making the operator think the target did not exist when
    in fact the directory configuration was broken.

    After the fix the parse error is remembered keyed by server name and
    `findLDAPUser` rethrows it as a query-time `BAD_ARGUMENTS` exception that
    names the offending server. `EXECUTE AS` surfaces that exception.
    """
    error = node_misconfigured_lookup.query_and_get_error(
        "EXECUTE AS janedoe SELECT 1",
        user="admin",
        password="qwerty",
    )
    assert "openldap" in error, error
    assert "user_dn_detection" in error or "misconfigured" in error.lower(), error
    assert "UNKNOWN_USER" not in error, error


def test_execute_as_role_mapping_user_dn_filter(started_cluster):
    """`@tiandiwonder` coverage gap on `src/Access/LDAPAccessStorage.cpp`: the
    other role-mapping tests use the `member={bind_dn}` filter, where
    `{bind_dn}` is constructed from the `bind_dn` template + `{user_name}` and
    does not depend on `user_dn_detection`. The documented AD-style mapping
    uses `{user_dn}`, which defaults to `final_bind_dn` and is overwritten by
    the `user_dn_detection` search result. The forced-lookup `find()` runs
    `user_dn_detection` before role search, so `{user_dn}` SHOULD resolve, but
    that path was not exercised in the existing tests. This test exercises it
    directly via `node_with_role_mapping_user_dn`, whose role-search filter
    uses `{user_dn}` rather than `{bind_dn}`.
    """
    node_with_role_mapping_user_dn.query(
        "CREATE ROLE IF NOT EXISTS pre_login_role_user_dn",
        user="admin",
        password="qwerty",
    )
    try:
        add_ldap_group(
            started_cluster,
            group_cn="clickhouse-pre_login_role_user_dn",
            member_cn="janedoe",
        )

        current_roles = node_with_role_mapping_user_dn.query(
            "EXECUTE AS janedoe SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="admin",
            password="qwerty",
        )
        assert current_roles.strip() == "pre_login_role_user_dn", current_roles
    finally:
        delete_ldap_group(
            started_cluster, group_cn="clickhouse-pre_login_role_user_dn"
        )
        node_with_role_mapping_user_dn.query(
            "DROP ROLE IF EXISTS pre_login_role_user_dn",
            user="admin",
            password="qwerty",
        )


def test_execute_as_refreshes_distributed_cache_poisoned_entry(started_cluster):
    """`@tiandiwonder` blocker on `src/Interpreters/Access/InterpreterExecuteAsQuery.cpp`
    + `src/Access/LDAPAccessStorage.cpp`: distributed `EXECUTE AS` over a
    cluster whose `<remote_servers>` entry has a `<secret>` authenticates
    remote shards via `AlwaysAllowCredentials{initial_user}`. The receiving
    shard's `LDAPAccessStorage::authenticateImpl` short-circuits in
    `areLDAPCredentialsValidNoLock` for `AlwaysAllowCredentials` and
    materializes the user with empty `users_external_roles` (no role mapping
    resolved). A subsequent local `EXECUTE AS <same user>` on that shard would
    hit the poisoned cache entry and run with only the common roles, silently
    losing the mapped roles. With `role_mapping` configured on every shard,
    this is a deterministic privilege-de-escalation for the headline guarantee
    of this PR.

    The fix detects an LDAP-storage-owned cached entry whose
    `users_external_roles[name].size()` does not match
    `role_search_params.size()` and refreshes it via the service-bind lookup
    on the next `EXECUTE AS`. Verified end-to-end by running a remote query
    that pre-poisons the cache, then a local `EXECUTE AS` that must surface
    the refreshed mapped role.
    """
    # Create the role to be mapped on both shards. The impersonated user must be
    # able to read from `clusterAllReplicas`, so the role gets `READ ON *.*`. This
    # privilege is what we later assert is in effect on shard2 by checking the
    # role name appears in `system.current_roles` after the cache refresh.
    for inst in (shard1_node, shard2_node):
        inst.query(
            "CREATE ROLE IF NOT EXISTS distributed_pre_login_role",
            user="admin",
            password="qwerty",
        )
        # Grant the impersonated `janedoe` enough privileges to run the
        # cluster-fanout query (`clusterAllReplicas` needs `SELECT` and
        # `SHOW COLUMNS` on the target table plus the `REMOTE` source access).
        # The test is about role-mapping refresh after cache poisoning, not the
        # specific privilege set.
        for grant in (
            "GRANT SELECT ON *.* TO distributed_pre_login_role",
            "GRANT SHOW COLUMNS ON *.* TO distributed_pre_login_role",
            "GRANT REMOTE ON *.* TO distributed_pre_login_role",
        ):
            inst.query(grant, user="admin", password="qwerty")
    try:
        add_ldap_group(
            started_cluster,
            group_cn="clickhouse-distributed_pre_login_role",
            member_cn="janedoe",
        )

        # Pre-poison shard2's cache: a distributed query that fans out under
        # `<secret>` causes shard2 to authenticate the remote half via
        # `AlwaysAllowCredentials{janedoe}`, materializing `janedoe` in
        # shard2's LDAP storage with empty `users_external_roles`.
        shard1_node.query(
            "EXECUTE AS janedoe SELECT count() FROM clusterAllReplicas('ldap_cluster', system.one)",
            user="admin",
            password="qwerty",
        )

        # Confirm shard2's LDAP storage has a `janedoe` entry now.
        post_remote_count = shard2_node.query(
            "SELECT count() FROM system.users WHERE name = 'janedoe' AND storage = 'ldap'",
            user="admin",
            password="qwerty",
        )
        assert post_remote_count.strip() == "1", post_remote_count

        # The local `EXECUTE AS` on shard2 must surface
        # `distributed_pre_login_role`. Without the cache-refresh fix the
        # poisoned entry has empty `users_external_roles` and the role would
        # be missing from `system.current_roles`.
        current_roles = shard2_node.query(
            "EXECUTE AS janedoe SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="admin",
            password="qwerty",
        )
        assert current_roles.strip() == "distributed_pre_login_role", current_roles
    finally:
        delete_ldap_group(
            started_cluster, group_cn="clickhouse-distributed_pre_login_role"
        )
        for inst in (shard1_node, shard2_node):
            inst.query(
                "DROP ROLE IF EXISTS distributed_pre_login_role",
                user="admin",
                password="qwerty",
            )


def test_execute_as_already_materialized_ldap_user_when_ldap_first(started_cluster):
    """`@tiandiwonder` wording-precision nit on `src/Interpreters/Access/InterpreterExecuteAsQuery.cpp`:
    the two-pass lookup does not make a local user win regardless of
    `user_directories` order. When the LDAP storage is ordered before
    `users_xml` AND the LDAP storage already has a same-name entry materialized
    (e.g. because a prior LDAP login on this server cached it), the first
    `find<User>(name)` returns the LDAP entry — exactly as the pre-PR
    `getID<User>` path did. The two-pass logic only prevents the FORCED LDAP
    lookup from materializing an LDAP entry on a global normal-lookup miss
    that would otherwise reach the local user. This test pins that behavior.

    Strategy:
      1. Authenticate `janedoe` against the LDAP fixture so the LDAP storage
         on `node_with_local_user_precedence` materializes the LDAP user.
      2. Run `EXECUTE AS janedoe` from `admin`. The first pass walks the
         storages in order. LDAP is ordered first AND has the entry, so pass 1
         returns the LDAP id. The local `janedoe` in `users.xml` is shadowed.
         This is identical to the pre-PR `getID<User>` resolution order.
    """
    # Materialize `janedoe` in the LDAP storage on this instance.
    assert node_with_local_user_precedence.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ).strip() == "janedoe"

    pre_ldap_count = node_with_local_user_precedence.query(
        "SELECT count() FROM system.users "
        "WHERE name = 'janedoe' AND storage = 'ldap'",
        user="admin",
        password="qwerty",
    )
    assert pre_ldap_count.strip() == "1", pre_ldap_count

    # First pass returns the LDAP entry because LDAP is ordered first; the
    # storage that owns the resolved id is `ldap`. The session runs as the
    # LDAP `janedoe`, NOT the local `janedoe` from `users_xml`. This is the
    # same precedence the pre-PR `getID<User>` path produced for an LDAP user
    # who had already been cached. The two-pass fix does not override that.
    result = node_with_local_user_precedence.query(
        "EXECUTE AS janedoe SELECT currentUser()",
        user="admin",
        password="qwerty",
    )
    assert result.strip() == "janedoe"


def test_execute_as_refresh_does_not_change_winning_storage_with_two_ldap_directories(
    started_cluster,
):
    """`@clickhouse-gh[bot]` 2026-06-05 inline review on
    `src/Interpreters/Access/InterpreterExecuteAsQuery.cpp`: the cache-refresh
    pass must run against the storage that won the first (non-forced) lookup,
    not globally. With two LDAP user-directories `[ldap_a, ldap_b, users_xml]`,
    once one of them has cached the target user and the other has not, pass 1
    correctly returns the cached entry, matching the pre-PR `getID<User>`
    precedence. A global `find<User>(name, force_external_lookup=true)` from
    that point would walk the storages in order and could materialize the
    target user in the previously empty LDAP storage via its service bind,
    returning that UUID instead, so `EXECUTE AS janedoe` would run with roles
    from the wrong directory.

    Strategy:
      1. Authenticate `janedoe` against this instance once. Whichever LDAP
         user-directory binds first materializes the entry; the other LDAP
         storage stays empty.
      2. `EXECUTE AS janedoe` from `admin`. Pass 1 resolves to the cached
         storage. The fix refreshes that storage only, leaving the empty one
         alone.
      3. Assert the empty LDAP storage is still empty after `EXECUTE AS`.
         Before the fix the global force_external_lookup would have
         materialized `janedoe` there as well, returning a different UUID and
         changing which directory wins.
    """
    # Materialize `janedoe` in whichever LDAP storage the auth chain reaches
    # first. Both directories point to the same OpenLDAP backend, so the bind
    # succeeds in the first one tried.
    login_result = node_with_two_ldap_directories.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    )
    assert login_result.strip() == "janedoe", login_result

    cached_storages = sorted(
        line.strip()
        for line in node_with_two_ldap_directories.query(
            "SELECT storage FROM system.users "
            "WHERE name = 'janedoe' AND storage IN ('ldap_a', 'ldap_b') "
            "ORDER BY storage",
            user="admin",
            password="qwerty",
        ).splitlines()
        if line.strip()
    )
    assert len(cached_storages) == 1, (
        "expected exactly one LDAP storage to have cached `janedoe` after "
        f"login, got {cached_storages!r}"
    )
    cached_storage = cached_storages[0]
    other_storage = "ldap_b" if cached_storage == "ldap_a" else "ldap_a"

    # The actual `EXECUTE AS`. Pass 1 resolves to `cached_storage`. The fix
    # refreshes only `cached_storage`'s entry. The buggy path would call a
    # global force_external_lookup that walks the directories in order and
    # could materialize `janedoe` in the previously empty `other_storage`.
    node_with_two_ldap_directories.query(
        "EXECUTE AS janedoe SELECT 1",
        user="admin",
        password="qwerty",
    )

    post_other = node_with_two_ldap_directories.query(
        f"SELECT count() FROM system.users "
        f"WHERE name = 'janedoe' AND storage = '{other_storage}'",
        user="admin",
        password="qwerty",
    )
    assert post_other.strip() == "0", (
        f"EXECUTE AS materialized `janedoe` in `{other_storage}` even "
        f"though pass 1 resolved to `{cached_storage}`: "
        f"got {post_other.strip()} entries in `{other_storage}`"
    )
    post_cached = node_with_two_ldap_directories.query(
        f"SELECT count() FROM system.users "
        f"WHERE name = 'janedoe' AND storage = '{cached_storage}'",
        user="admin",
        password="qwerty",
    )
    assert post_cached.strip() == "1", post_cached


def test_execute_as_wrong_lookup_password_throws_ldap_error(started_cluster):
    """Bot concern on `src/Access/LDAPClient.cpp`: when the service-bind fails with
    `LDAP_INVALID_CREDENTIALS` (e.g. mistyped or rotated `lookup_password`), the
    caller used to receive the same `false` that signals "the user does not exist
    in the directory", and EXECUTE AS would collapse to the canonical
    `UNKNOWN_USER` path. That fails the feature open diagnostically and hides the
    actual operator error.

    After the fix, an invalid service-bind credential throws an LDAP configuration
    error from `openConnection(BindMode::Service)`. EXECUTE AS surfaces that error
    so the operator can identify the broken `lookup_bind_dn` /
    `lookup_password`. The target user (`janedoe`) is genuinely present in LDAP, so
    `UNKNOWN_USER` is specifically the wrong answer here.
    """
    error = node_bad_lookup_password.query_and_get_error(
        "EXECUTE AS janedoe SELECT 1",
        user="admin",
        password="qwerty",
    )
    # The exception should mention the LDAP service-bind problem and must NOT
    # collapse to `UNKNOWN_USER`. The exact text comes from `LDAPClient::openConnection`.
    assert "LDAP_ERROR" in error or "lookup" in error.lower(), error
    assert "UNKNOWN_USER" not in error, error


def test_execute_as_static_user_dn_detection_is_rejected(started_cluster):
    """`clickhouse-gh[bot]` blocker on `src/Access/ExternalAuthenticators.cpp`:
    `lookup_bind_dn` requires `user_dn_detection` to confirm the user exists, but
    that requirement was satisfied as soon as the section was *present*. A
    target-independent search (e.g. a static `search_filter` like `(cn=janedoe)`
    over a static `base_dn`) returns the same single entry for every requested
    name, so `final_user_dn` would be set from that entry and `LDAPAccessStorage`
    would materialize a phantom user under the requested name regardless of which
    name was being impersonated.

    After the fix `parseLDAPServer` requires `user_dn_detection.base_dn` or
    `user_dn_detection.search_filter` to depend on the requested user via
    `{user_name}` (or `{bind_dn}`/`{user_dn}` when `bind_dn` itself depends on
    `{user_name}`). The saved `BAD_ARGUMENTS` is rethrown by `findLDAPUser` so
    `EXECUTE AS` surfaces the configuration error rather than silently
    materializing the entry.
    """
    error = node_static_user_dn_detection.query_and_get_error(
        # Pick a name that genuinely is not in the directory; with the bug the
        # static `(cn=janedoe)` filter would still return the `janedoe` entry,
        # set `final_user_dn` to its DN, and `findImpl` would materialize a
        # `definitely_not_in_ldap` user mapped to `janedoe`'s DN.
        "EXECUTE AS definitely_not_in_ldap SELECT 1",
        user="admin",
        password="qwerty",
    )
    assert "openldap" in error, error
    assert "user_dn_detection" in error, error
    assert "{user_name}" in error or "user_name" in error, error
    assert "UNKNOWN_USER" not in error, error
