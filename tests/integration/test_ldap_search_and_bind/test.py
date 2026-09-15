"""
Search-and-bind authentication against a strict OpenLDAP fixture.

The `openldap_strict` container (tests/integration/compose/docker_compose_ldap_strict.yml,
bootstrapped by ci/docker/integration/runner/misc/openldap_strict/setup_strict.sh)
refuses anonymous binds, lets only the service account
`cn=svc.clickhouse,ou=service,dc=example,dc=org` read `ou=groups` and `memberOf`, and
contains users whose names need RFC 4514/4515 escaping plus a `uid` that exists twice.

Instances:
  - `instance`: `bind_dn` = `{user_dn}` + `lookup_bind_dn`/`lookup_password` (search and
    bind), `verification_cooldown` = 300, reverse `role_mapping` search on `ou=groups`.
  - `instance_bad_lookup`: same with a wrong `lookup_password`.
  - `instance_parse_error`: server `broken` (`lookup_bind_dn` without `lookup_password`)
    backing the directory, plus a valid legacy server `plain` for a local user.
  - `instance_mode2`: `bind_dn` template + `lookup_bind_dn` (direct bind, searches as the
    service account).
  - `instance_mode2_bind_dn_base`: like `instance_mode2`, but `user_dn_detection.base_dn` is
    `{bind_dn}`, so the detection depends on the login only through the `bind_dn` template.

Clients always receive the generic `Authentication failed` message; the exact reason is
only in the server log, which is what the assertions below inspect.
"""

import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from helpers.test_tools import TSV, assert_logs_contain_with_retry

LDAP_HOST = "openldap_strict"
LDAP_PORT = 1389
LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"
LDAP_SERVICE_BIND_DN = "cn=svc.clickhouse,ou=service,dc=example,dc=org"
LDAP_SERVICE_PASSWORD = "svcsecret"
LDAP_SERVER_NAME = "openldap_strict"
LOOKUP_BIND_FAILED = (
    f"LDAP lookup bind as '{LDAP_SERVICE_BIND_DN}' failed for server '{LDAP_SERVER_NAME}':"
    " invalid credentials"
)

DOCKER_COMPOSE_PATH = get_docker_compose_path()
CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs")

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/ldap_search_and_bind.xml"],
    user_configs=["configs/users.xml"],
)

instance_bad_lookup = cluster.add_instance(
    "instance_bad_lookup",
    main_configs=["configs/ldap_bad_lookup.xml"],
    user_configs=["configs/users.xml"],
)

instance_parse_error = cluster.add_instance(
    "instance_parse_error",
    main_configs=["configs/ldap_parse_error.xml"],
    user_configs=["configs/users.xml"],
)

instance_mode2 = cluster.add_instance(
    "instance_mode2",
    main_configs=["configs/ldap_mode2.xml"],
    user_configs=["configs/users.xml"],
)

instance_mode2_bind_dn_base = cluster.add_instance(
    "instance_mode2_bind_dn_base",
    main_configs=["configs/ldap_mode2_bind_dn_base.xml"],
    user_configs=["configs/users.xml"],
)


def ldap_exec(command):
    """Run a shell command inside the `openldap_strict` container (raises on failure)."""
    return cluster.exec_in_container(
        cluster.get_instance_docker_id(LDAP_HOST), ["bash", "-c", command], user="root"
    )


def wait_openldap_strict_ready(timeout=180):
    start = time.time()
    attempts = 0
    logging.info("Waiting for openldap_strict readiness")
    while time.time() - start < timeout:
        attempts += 1
        try:
            ldap_exec(
                "test -f /tmp/.openldap-initialized"
                f" && /opt/bitnami/openldap/bin/ldapsearch -x -H ldap://localhost:{LDAP_PORT}"
                f" -D {LDAP_SERVICE_BIND_DN} -w {LDAP_SERVICE_PASSWORD}"
                " -b ou=users,dc=example,dc=org '(uid=janedoe)' dn"
                " | grep -c '^dn: cn=janedoe,ou=users,dc=example,dc=org$'"
                " | grep 1 >> /dev/null"
            )
            logging.info("openldap_strict is ready")
            return
        except Exception as ex:
            if attempts % 10 == 0:
                logging.info(
                    "openldap_strict not ready after %s attempts: %s", attempts, str(ex)
                )
            else:
                logging.debug("openldap_strict not ready yet: %s", str(ex))
            time.sleep(1)
    raise Exception("Timed out waiting for openldap_strict")


def ldap_add_group(group_cn, member_cns):
    """Add a `groupOfNames` under `ou=groups` (readable by the service account only)."""
    members = "".join(
        f"member: cn={member_cn},ou=users,dc=example,dc=org\n"
        for member_cn in member_cns
    )
    ldap_exec(
        f'echo "dn: cn={group_cn},ou=groups,dc=example,dc=org\n'
        "objectClass: top\n"
        "objectClass: groupOfNames\n"
        f"cn: {group_cn}\n"
        f'{members}" | ldapadd -x -H ldap://localhost:{LDAP_PORT}'
        f' -D "{LDAP_ADMIN_BIND_DN}" -w {LDAP_ADMIN_PASSWORD}'
    )


def ldap_delete_group(group_cn):
    ldap_exec(
        f'ldapdelete -x -H ldap://localhost:{LDAP_PORT} -D "{LDAP_ADMIN_BIND_DN}"'
        f' -w {LDAP_ADMIN_PASSWORD} "cn={group_cn},ou=groups,dc=example,dc=org"'
    )


def ldap_set_service_password(new_password):
    ldap_exec(
        f'ldappasswd -x -H ldap://localhost:{LDAP_PORT} -D "{LDAP_ADMIN_BIND_DN}"'
        f' -w {LDAP_ADMIN_PASSWORD} -s {new_password} "{LDAP_SERVICE_BIND_DN}"'
    )


def count_in_log(node, substring):
    return int(node.count_in_log(substring).strip() or 0)


def wait_count_in_log(node, substring, expected, retry_count=40, sleep_time=0.5):
    for _ in range(retry_count):
        if count_in_log(node, substring) >= expected:
            return
        time.sleep(sleep_time)
    raise AssertionError(
        f"expected at least {expected} occurrences of {substring!r} in the log of {node.name}, "
        f"got {count_in_log(node, substring)}"
    )


def login_fails_without_ldap_error(node, user, password):
    """The login must be rejected as a plain authentication failure: no `LDAP_ERROR` is
    logged for it, so the following user directories keep their chance to authenticate.
    """
    failed_before = count_in_log(node, "Authentication failed")
    ldap_errors_before = count_in_log(node, "LDAP_ERROR")

    error = node.query_and_get_error(
        "SELECT currentUser()", user=user, password=password
    )
    assert "Authentication failed" in error, error

    wait_count_in_log(node, "Authentication failed", failed_before + 1)
    assert count_in_log(node, "LDAP_ERROR") == ldap_errors_before
    return error


def read_config(name):
    with open(os.path.join(CONFIGS_DIR, name)) as f:
        return f.read()


def reload_config(node, config_name, content):
    node.replace_config(f"/etc/clickhouse-server/config.d/{config_name}", content)
    node.query("SYSTEM RELOAD CONFIG", user="common_user", password="qwerty")


def clear_ldap_cache(node):
    """`SYSTEM RELOAD CONFIG` re-applies `ldap_servers` and drops every
    `verification_cooldown` cache entry, so the next login is guaranteed to reach LDAP.
    """
    node.query("SYSTEM RELOAD CONFIG", user="common_user", password="qwerty")


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    docker_compose_ldap_strict = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_ldap_strict.yml"
    )
    try:
        cluster.start()

        # The strict LDAP server is started outside of `cluster.start`, like the referral
        # server in test_ldap_follow_referrals, so it does not disturb the shared fixture.
        run_and_check(
            cluster.compose_cmd(
                "-f",
                docker_compose_ldap_strict,
                "up",
                "--force-recreate",
                "-d",
                "--no-build",
            )
        )
        wait_openldap_strict_ready()

        yield cluster
    finally:
        run_and_check(
            cluster.compose_cmd(
                "-f",
                docker_compose_ldap_strict,
                "down",
                "--volumes",
            ),
            nothrow=True,
        )
        cluster.shutdown()


def test_search_and_bind_authenticates(ldap_cluster):
    """Mode 3: the login is not part of any DN template; the user is found by
    `user_dn_detection` under the service account and the password is verified by
    binding as the DN that was found."""
    assert instance.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])

    assert instance.query(
        "SELECT name, storage, auth_type FROM system.users WHERE name = 'janedoe'",
        user="common_user",
        password="qwerty",
    ) == TSV([["janedoe", "ldap", "['ldap']"]])


def test_wrong_password_and_unknown_user_are_authentication_failures(ldap_cluster):
    # Wrong password: the service account finds the DN, the user bind is rejected.
    login_fails_without_ldap_error(instance, "janedoe", "wrong")

    # Unknown user: `user_dn_detection` returns nothing, no user bind is attempted.
    login_fails_without_ldap_error(instance, "nosuchuser", "qwerty")


def test_ambiguous_user_is_an_ldap_error(ldap_cluster):
    """`uid=dupuser` exists under both `ou=users` and `ou=service`, so the detection
    yields two DNs and the login must not guess which one to bind as."""
    error = instance.query_and_get_error(
        "SELECT currentUser()", user="dupuser", password="qwerty"
    )
    assert "Authentication failed" in error, error
    assert_logs_contain_with_retry(
        instance, "Failed to detect user DN: more than one entry in the search results"
    )


def test_login_is_escaped_once(ldap_cluster):
    """`special(user)*` needs RFC 4515 filter escaping; `o'neil,doe` and `a=b` contain
    RFC 4514 DN-special characters that used to be escaped for the DN first and then
    again for the filter (`a\\5C=b`), so they never matched."""
    for user in ["special(user)*", "o'neil,doe", "a=b"]:
        # `TSVRaw`: the TabSeparated format would escape the quote in `o'neil,doe`.
        assert instance.query(
            "SELECT currentUser() FORMAT TSVRaw", user=user, password="qwerty"
        ) == TSV([[user]]), user


def test_filter_injection_shaped_login_is_rejected(ldap_cluster):
    """A login shaped like a filter fragment must be escaped into a harmless literal,
    so it neither matches another entry nor breaks the filter."""
    login_fails_without_ldap_error(instance, "a*b)(uid=*", "qwerty")
    assert not instance.contains_in_log("Bad search filter")


def test_role_mapping_runs_as_service_account(ldap_cluster):
    """`ou=groups` is unreadable for users (ACL `{1}` in setup_strict.sh): the mapped role
    can only appear because the search runs after the re-bind as the service account."""
    # A cached login would reuse the role set of an earlier login without contacting LDAP.
    clear_ldap_cache(instance)

    instance.query("DROP ROLE IF EXISTS role_1", user="common_user", password="qwerty")
    instance.query("CREATE ROLE role_1", user="common_user", password="qwerty")
    try:
        ldap_add_group("clickhouse-role_1", ["johndoe"])

        assert instance.query(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        ) == TSV([["johndoe"]])

        assert instance.query(
            "SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="johndoe",
            password="qwertz",
        ) == TSV([["role_1"]])
    finally:
        ldap_delete_group("clickhouse-role_1")
        instance.query(
            "DROP ROLE IF EXISTS role_1", user="common_user", password="qwerty"
        )


def test_wrong_lookup_password_is_an_ldap_error(ldap_cluster):
    """A rejected service account bind is an operator error, so it is reported as
    `LDAP_ERROR` naming the lookup bind, not folded into "no such user"."""
    error = instance_bad_lookup.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    )
    assert "Authentication failed" in error, error
    assert_logs_contain_with_retry(instance_bad_lookup, LOOKUP_BIND_FAILED)
    assert instance_bad_lookup.contains_in_log("LDAP_ERROR")


def test_parse_error_fails_closed_at_login(ldap_cluster):
    """A server rejected by `parseLDAPServer` stays rejected: logins through the
    directory fail with the original message, other servers keep working."""
    assert_logs_contain_with_retry(instance_parse_error, "Could not parse LDAP server")
    assert instance_parse_error.contains_in_log(
        "Both 'lookup_bind_dn' and 'lookup_password' must be specified together"
    )

    error = instance_parse_error.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    )
    assert "Authentication failed" in error, error
    assert_logs_contain_with_retry(
        instance_parse_error,
        "LDAP server 'broken' is misconfigured: Both 'lookup_bind_dn' and 'lookup_password' must be specified together",
    )

    # The valid legacy server `plain` in the same section is unaffected.
    instance_parse_error.query(
        "CREATE USER janedoe IDENTIFIED WITH ldap SERVER 'plain'",
        user="common_user",
        password="qwerty",
    )
    try:
        assert instance_parse_error.query(
            "SELECT currentUser()", user="janedoe", password="qwerty"
        ) == TSV([["janedoe"]])
    finally:
        instance_parse_error.query(
            "DROP USER IF EXISTS janedoe", user="common_user", password="qwerty"
        )


def test_duplicate_server_name_fails_closed(ldap_cluster):
    """Two `ldap_servers` entries sharing a name (Poco keys `broken` and `broken[1]`): the
    first parses, the second is rejected with "Multiple LDAP servers with the same name are
    not allowed". The name must then be unusable altogether, not served from the first entry,
    even though that entry alone would be a valid search-and-bind configuration."""
    original_config = read_config("ldap_parse_error.xml")
    valid_server = """
        <broken>
            <host>openldap_strict</host>
            <port>1389</port>
            <enable_tls>no</enable_tls>
            <lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>
            <lookup_password>svcsecret</lookup_password>
            <bind_dn>{user_dn}</bind_dn>
            <user_dn_detection>
                <base_dn>dc=example,dc=org</base_dn>
                <search_filter>(&amp;(objectClass=inetOrgPerson)(uid={user_name}))</search_filter>
            </user_dn_detection>
        </broken>"""
    duplicate_config = f"""<clickhouse>
    <ldap_servers>{valid_server}{valid_server}
    </ldap_servers>
    <user_directories>
        <ldap>
            <server>broken</server>
        </ldap>
    </user_directories>
</clickhouse>
"""
    expected = "Multiple LDAP servers with the same name are not allowed"
    try:
        reload_config(instance_parse_error, "ldap_parse_error.xml", duplicate_config)
        assert_logs_contain_with_retry(instance_parse_error, expected)

        error = instance_parse_error.query_and_get_error(
            "SELECT currentUser()", user="janedoe", password="qwerty"
        )
        assert "Authentication failed" in error, error
        assert_logs_contain_with_retry(
            instance_parse_error, f"LDAP server 'broken' is misconfigured: {expected}"
        )

        # The forced lookup of `EXECUTE AS` fails closed with the same reason instead of
        # resolving the user through the entry that parsed.
        error = instance_parse_error.query_and_get_error(
            "EXECUTE AS janedoe SELECT 1", user="common_user", password="qwerty"
        )
        assert "is misconfigured" in error, error
        assert expected in error, error
        assert "UNKNOWN_USER" not in error, error
    finally:
        reload_config(instance_parse_error, "ldap_parse_error.xml", original_config)


def test_service_password_rotation(ldap_cluster):
    """`verification_cooldown` answers cached logins without LDAP; every uncached login
    fails closed on the lookup bind until the configuration is reloaded; the lookup
    credentials are part of the cache key, so the reload invalidates everything."""
    original_config = read_config("ldap_search_and_bind.xml")
    rotated_config = original_config.replace(LDAP_SERVICE_PASSWORD, "rotatedsecret")
    assert rotated_config != original_config

    # Start from an empty cache so that this login creates the entry of `janedoe` (a cache
    # hit does not refresh the timestamp, so an entry from an earlier test could expire
    # in the middle of this test).
    clear_ldap_cache(instance)
    assert instance.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])

    try:
        ldap_set_service_password("rotatedsecret")

        # Cached: no LDAP contact, still accepted.
        assert instance.query(
            "SELECT currentUser()", user="janedoe", password="qwerty"
        ) == TSV([["janedoe"]])

        # Uncached: the lookup bind runs first and fails with the old password.
        ldap_errors_before = count_in_log(instance, "LDAP_ERROR")
        error = instance.query_and_get_error(
            "SELECT currentUser()", user="uncached_user", password="qwerty"
        )
        assert "Authentication failed" in error, error
        wait_count_in_log(instance, "LDAP_ERROR", ldap_errors_before + 1)
        assert instance.contains_in_log(LOOKUP_BIND_FAILED)

        # Reload with the new password: both a cached and an uncached user succeed.
        reload_config(instance, "ldap_search_and_bind.xml", rotated_config)
        for user, password in [("janedoe", "qwerty"), ("johndoe", "qwertz")]:
            assert instance.query(
                "SELECT currentUser()", user=user, password=password
            ) == TSV([[user]])
    finally:
        ldap_set_service_password(LDAP_SERVICE_PASSWORD)
        reload_config(instance, "ldap_search_and_bind.xml", original_config)

    assert instance.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])


def test_direct_bind_with_lookup_identity_searches_as_service_account(ldap_cluster):
    """Mode 2: the password is verified against the `bind_dn` template, then the
    connection is re-bound as the service account, which is the only identity that may
    read `ou=groups`."""
    instance_mode2.query(
        "DROP ROLE IF EXISTS role_m2", user="common_user", password="qwerty"
    )
    instance_mode2.query("CREATE ROLE role_m2", user="common_user", password="qwerty")
    try:
        ldap_add_group("clickhouse-role_m2", ["janedoe"])

        assert instance_mode2.query(
            "SELECT currentUser()", user="janedoe", password="qwerty"
        ) == TSV([["janedoe"]])

        assert instance_mode2.query(
            "SELECT role_name FROM system.current_roles ORDER BY role_name",
            user="janedoe",
            password="qwerty",
        ) == TSV([["role_m2"]])

        login_fails_without_ldap_error(instance_mode2, "janedoe", "wrong")
    finally:
        ldap_delete_group("clickhouse-role_m2")
        instance_mode2.query(
            "DROP ROLE IF EXISTS role_m2", user="common_user", password="qwerty"
        )


def test_detection_base_from_bind_dn_treats_missing_base_as_unknown_user(ldap_cluster):
    """Mode 2 with `user_dn_detection.base_dn` = `{bind_dn}`: the base depends on the login
    only through the `bind_dn` template. For an unknown user it does not exist in the
    directory, which answers the search with `LDAP_NO_SUCH_OBJECT`; that is the same "user
    not found" signal as for `{user_name}` written directly into `base_dn`, so a login must be
    a plain authentication failure and `EXECUTE AS` must give `UNKNOWN_USER`, never
    `LDAP_ERROR`. `common_user` has `access_management`, which includes `IMPERSONATE`."""
    # Known users resolve through the substituted base, both at login and on the forced
    # lookup of `EXECUTE AS` (johndoe has never logged in on this instance).
    assert instance_mode2_bind_dn_base.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])
    assert instance_mode2_bind_dn_base.query(
        "EXECUTE AS johndoe SELECT currentUser()",
        user="common_user",
        password="qwerty",
    ) == TSV([["johndoe"]])

    login_fails_without_ldap_error(instance_mode2_bind_dn_base, "nosuchuser", "qwerty")

    ldap_errors_before = count_in_log(instance_mode2_bind_dn_base, "LDAP_ERROR")
    error = instance_mode2_bind_dn_base.query_and_get_error(
        "EXECUTE AS nosuchuser SELECT 1", user="common_user", password="qwerty"
    )
    assert "UNKNOWN_USER" in error or "There is no user" in error, error
    assert "LDAP_ERROR" not in error, error
    assert "No such object" not in error, error
    assert count_in_log(instance_mode2_bind_dn_base, "LDAP_ERROR") == ldap_errors_before


def test_neither_bind_dn_nor_lookup_bind_dn_is_rejected(ldap_cluster):
    """Without any bind DN the client would perform an unauthenticated bind."""
    original_config = read_config("ldap_parse_error.xml")
    neither_config = """<clickhouse>
    <ldap_servers>
        <broken>
            <host>openldap_strict</host>
            <port>1389</port>
            <enable_tls>no</enable_tls>
        </broken>
        <plain>
            <host>openldap_strict</host>
            <port>1389</port>
            <enable_tls>no</enable_tls>
            <bind_dn>cn={user_name},ou=users,dc=example,dc=org</bind_dn>
        </plain>
    </ldap_servers>
    <user_directories>
        <ldap>
            <server>broken</server>
        </ldap>
    </user_directories>
</clickhouse>
"""
    try:
        reload_config(instance_parse_error, "ldap_parse_error.xml", neither_config)
        assert_logs_contain_with_retry(
            instance_parse_error,
            "Either 'bind_dn' or 'lookup_bind_dn' must be specified",
        )

        error = instance_parse_error.query_and_get_error(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        )
        assert "Authentication failed" in error, error
        assert_logs_contain_with_retry(
            instance_parse_error,
            "LDAP server 'broken' is misconfigured: Either 'bind_dn' or 'lookup_bind_dn' must be specified",
        )
    finally:
        reload_config(instance_parse_error, "ldap_parse_error.xml", original_config)


def test_search_and_bind_requires_dn_attribute(ldap_cluster):
    """With `bind_dn` = `{user_dn}` the value returned by `user_dn_detection` is bound
    as a DN, so an `attribute` other than `dn` is rejected at parse time."""
    original_config = read_config("ldap_parse_error.xml")
    uid_attribute_config = original_config.replace(
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>",
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>\n"
        "            <lookup_password>svcsecret</lookup_password>",
    ).replace(
        "<base_dn>dc=example,dc=org</base_dn>",
        "<base_dn>dc=example,dc=org</base_dn>\n                <attribute>uid</attribute>",
    )
    assert uid_attribute_config != original_config
    try:
        reload_config(
            instance_parse_error, "ldap_parse_error.xml", uid_attribute_config
        )
        assert_logs_contain_with_retry(
            instance_parse_error,
            "'user_dn_detection.attribute' must be 'dn' when 'bind_dn' = '{user_dn}', got 'uid'",
        )

        error = instance_parse_error.query_and_get_error(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        )
        assert "Authentication failed" in error, error
        assert_logs_contain_with_retry(
            instance_parse_error,
            "LDAP server 'broken' is misconfigured: 'user_dn_detection.attribute' must be 'dn'",
        )
    finally:
        reload_config(instance_parse_error, "ldap_parse_error.xml", original_config)


def test_search_and_bind_requires_user_name_in_detection(ldap_cluster):
    """With `bind_dn` = `{user_dn}` a static `user_dn_detection` would bind every login as
    the same entry; the rejection must carry the search-and-bind message, not the generic
    hint to use `{bind_dn}`/`{user_dn}` (which is not allowed in this mode)."""
    original_config = read_config("ldap_parse_error.xml")
    static_filter_config = original_config.replace(
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>",
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>\n"
        "            <lookup_password>svcsecret</lookup_password>",
    ).replace(
        "<search_filter>(&amp;(objectClass=inetOrgPerson)(uid={user_name}))</search_filter>",
        "<search_filter>(&amp;(objectClass=inetOrgPerson)(uid=janedoe))</search_filter>",
    )
    assert static_filter_config != original_config
    expected = (
        "'bind_dn' = '{user_dn}' requires 'user_dn_detection.base_dn' or"
        " 'user_dn_detection.search_filter' to contain '{user_name}'"
    )
    try:
        reload_config(
            instance_parse_error, "ldap_parse_error.xml", static_filter_config
        )
        assert_logs_contain_with_retry(instance_parse_error, expected)
        assert not instance_parse_error.contains_in_log(
            "or use '{bind_dn}'/'{user_dn}' with a 'bind_dn' template"
        )

        error = instance_parse_error.query_and_get_error(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        )
        assert "Authentication failed" in error, error
        assert_logs_contain_with_retry(
            instance_parse_error, f"LDAP server 'broken' is misconfigured: {expected}"
        )
    finally:
        reload_config(instance_parse_error, "ldap_parse_error.xml", original_config)


def test_nonexistent_detection_base_dn_is_an_ldap_error(ldap_cluster):
    """A static `base_dn` that does not exist (mistyped naming context) makes the directory
    answer `user_dn_detection` with `LDAP_NO_SUCH_OBJECT`. That is a misconfiguration and
    must be logged as `LDAP_ERROR`; only a `base_dn` that depends on the login (`{user_name}`,
    or `{bind_dn}`/`{user_dn}` from a `bind_dn` template) may treat it as "user not found"."""
    original_config = read_config("ldap_parse_error.xml")
    nonexistent_base_config = original_config.replace(
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>",
        "<lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>\n"
        "            <lookup_password>svcsecret</lookup_password>",
    ).replace(
        "<base_dn>dc=example,dc=org</base_dn>",
        "<base_dn>dc=nonexistent,dc=org</base_dn>",
    )
    assert nonexistent_base_config != original_config
    parse_errors_before = count_in_log(
        instance_parse_error, "Could not parse LDAP server"
    )
    try:
        reload_config(
            instance_parse_error, "ldap_parse_error.xml", nonexistent_base_config
        )
        # The configuration itself is valid, so nothing is rejected at parse time.
        assert (
            count_in_log(instance_parse_error, "Could not parse LDAP server")
            == parse_errors_before
        )

        ldap_errors_before = count_in_log(instance_parse_error, "LDAP_ERROR")
        error = instance_parse_error.query_and_get_error(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        )
        assert "Authentication failed" in error, error
        wait_count_in_log(instance_parse_error, "LDAP_ERROR", ldap_errors_before + 1)
        assert instance_parse_error.contains_in_log("No such object")
    finally:
        reload_config(instance_parse_error, "ldap_parse_error.xml", original_config)


def test_user_directories_do_not_expose_the_service_password(ldap_cluster):
    params = instance.query(
        "SELECT params FROM system.user_directories WHERE type = 'ldap'",
        user="common_user",
        password="qwerty",
    )
    assert '"server":"openldap_strict"' in params, params
    assert LDAP_SERVICE_PASSWORD not in params, params
    assert "lookup" not in params, params
