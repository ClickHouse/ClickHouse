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

The tests in this file rely on test execution order: `cluster.start` runs once for
the module, leaving the LDAP storage's in-memory cache empty, so the first test
exercises the not-yet-logged-in path before any subsequent test materializes those
users.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/ldap.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_ldap=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


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
