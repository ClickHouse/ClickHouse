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
tests on `node_with_role_mapping`, `node_bad_lookup_password`, and
`node_with_local_user_precedence` exercise the role-mapping, misconfiguration, and
local-vs-LDAP precedence variants on instances that no other test logs into, so
their LDAP cache stays empty for the duration of the module.
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
