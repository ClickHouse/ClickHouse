"""
Regression test for https://github.com/ClickHouse/ClickHouse/issues/99574 .

`EXECUTE AS <ldap_user>` must work for users authenticated through an external LDAP
user directory even when the target user has not yet logged in since the most recent
server restart. The LDAP user-directory only populates its in-memory user entry on
first successful authentication, so before the fix the AccessControl name lookup
returned `UNKNOWN_USER` and impersonation failed with:

    Code: 192. DB::Exception: There is no user `janedoe` in `user directories`.

The fix makes the EXECUTE AS interpreter fall back to the LDAP storage's
authenticate-side materialization path (driven by `AlwaysAllowCredentials`) so that
the user becomes resolvable for impersonation regardless of prior login history.

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
    drives the LDAP storage's authenticate path with `AlwaysAllowCredentials`, which
    materializes the user entry without contacting LDAP, and impersonation succeeds.
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
    """Security check: the LDAP fallback must not bypass the IMPERSONATE access check.

    `restricted` is a users.xml user without `access_management`, so it has no
    IMPERSONATE grant on any user. EXECUTE AS must fail with an access denied error
    before the LDAP fallback has a chance to materialize the target user.
    """
    error = node.query_and_get_error(
        "EXECUTE AS janedoe SELECT 1",
        user="restricted",
        password="qwerty",
    )
    assert "ACCESS_DENIED" in error or "Not enough privileges" in error
