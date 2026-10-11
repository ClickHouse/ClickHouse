"""
Tests for `exclude_users` in the `ldap` user directory.

The LDAP directory is declared FIRST on both nodes and excludes `janedoe` and
`app_user1`. `janedoe` exists in the shared LDAP fixture (password `qwerty`), so
without the exclusion the directory would serve her and the local definitions in the
storages that follow it would never be reached. `instance_a` defines both excluded names
locally in `users.xml`; `instance_b` defines only `app_user1`, so on that node an
excluded `janedoe` has nowhere to fall through to and every login attempt must fail
closed.

The LDAP server is configured with `lookup_bind_dn`, so the forced lookup that `EXECUTE AS`
performs for a target who has not logged in yet (`LDAPAccessStorage::findImpl` with
`force_external_lookup`) is live on both nodes: a non-excluded LDAP user can be impersonated
before ever logging in. The `test_execute_as_*` tests check that the exclusion refuses that
lookup for `janedoe` before the server is contacted.

The last test restarts `instance_b` with an invalid `exclude_users` entry, which
discards its in-memory LDAP directory, so it must stay last in this module.
"""

import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

LDAP_DIRECTORY_CONFIG_PATH = "/etc/clickhouse-server/config.d/ldap_directory.xml"

cluster = ClickHouseCluster(__file__)

instance_a = cluster.add_instance(
    "instance_a",
    main_configs=["configs/ldap_directory.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users_a.xml"],
    stay_alive=True,
    with_ldap=True,
)

instance_b = cluster.add_instance(
    "instance_b",
    main_configs=["configs/ldap_directory.xml", "configs/remote_servers.xml"],
    user_configs=["configs/users_b.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def add_ldap_group(ldap_cluster, group_cn, member_cn):
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
        f"test_ldap_exclude_users code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


def delete_ldap_group(ldap_cluster, group_cn):
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """ldapdelete -r 'cn={group_cn},dc=example,dc=org' \
-H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
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
        f"test_ldap_exclude_users code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


def add_ldap_user(ldap_cluster, user_cn):
    # No `userPassword`: the entry only has to be found by the `user_dn_detection` search
    # that the service bind runs, it never logs in itself.
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """echo "dn: cn={user_cn},ou=users,dc=example,dc=org
objectClass: top
objectClass: person
objectClass: organizationalPerson
objectClass: inetOrgPerson
cn: {user_cn}
sn: {user_cn}" | \
ldapadd -H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
    """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                user_cn=user_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_exclude_users code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


def delete_ldap_user(ldap_cluster, user_cn):
    code, (stdout, stderr) = ldap_cluster.ldap_container.exec_run(
        [
            "sh",
            "-c",
            """ldapdelete 'cn={user_cn},ou=users,dc=example,dc=org' \
-H ldap://{host}:{port} -D "{admin_bind_dn}" -x -w {admin_password}
            """.format(
                host=ldap_cluster.ldap_host,
                port=ldap_cluster.ldap_port,
                admin_bind_dn=LDAP_ADMIN_BIND_DN,
                admin_password=LDAP_ADMIN_PASSWORD,
                user_cn=user_cn,
            ),
        ],
        demux=True,
    )
    logging.debug(
        f"test_ldap_exclude_users code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0


def query_as_admin(instance, sql):
    return instance.query(sql, user="common_user", password="qwerty")


def ldap_user_count(instance, user_name):
    return query_as_admin(
        instance,
        f"SELECT count() FROM system.users WHERE name = '{user_name}' AND storage = 'ldap'",
    ).strip()


def count_skip_log_lines(instance, user_name):
    return len(instance.grep_in_log(f"Skipping excluded user {user_name}").splitlines())


def setup_distributed_tables():
    for instance in (instance_a, instance_b):
        query_as_admin(instance, "DROP TABLE IF EXISTS local_table SYNC")
        query_as_admin(
            instance,
            "CREATE TABLE local_table (id UInt32) ENGINE = MergeTree() ORDER BY id",
        )
    query_as_admin(instance_b, "INSERT INTO local_table VALUES (1), (2), (3)")
    query_as_admin(instance_a, "DROP TABLE IF EXISTS distributed_table SYNC")
    query_as_admin(
        instance_a,
        "CREATE TABLE distributed_table AS local_table "
        "ENGINE = Distributed(test_ldap_cluster, default, local_table)",
    )


def drop_distributed_tables():
    query_as_admin(instance_a, "DROP TABLE IF EXISTS distributed_table SYNC")
    for instance in (instance_a, instance_b):
        query_as_admin(instance, "DROP TABLE IF EXISTS local_table SYNC")


def test_excluded_user_is_not_served_by_ldap_directory():
    # `qwerty` is janedoe's LDAP password. The LDAP directory skips her without contacting
    # the server, so the local definition (password `local`) is the only one left to check.
    assert "janedoe: Authentication failed" in instance_a.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    )
    assert instance_a.contains_in_log("Skipping excluded user janedoe")
    assert ldap_user_count(instance_a, "janedoe") == "0"


def test_excluded_user_falls_through_to_local_definition():
    assert instance_a.query(
        "SELECT currentUser()", user="janedoe", password="local"
    ) == TSV([["janedoe"]])
    assert query_as_admin(
        instance_a, "SELECT storage FROM system.users WHERE name = 'janedoe'"
    ) == TSV([["users_xml"]])
    assert ldap_user_count(instance_a, "janedoe") == "0"


def test_excluded_user_without_local_definition_fails_closed():
    assert "janedoe: Authentication failed" in instance_b.query_and_get_error(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    )
    assert instance_b.contains_in_log("Skipping excluded user janedoe")
    assert ldap_user_count(instance_b, "janedoe") == "0"


def test_non_excluded_ldap_user_authenticates():
    for instance in (instance_a, instance_b):
        assert instance.query(
            "SELECT currentUser()", user="johndoe", password="qwertz"
        ) == TSV([["johndoe"]])
        assert ldap_user_count(instance, "johndoe") == "1"


def test_excluded_application_user_uses_local_secret():
    for instance in (instance_a, instance_b):
        assert instance.query(
            "SELECT currentUser()", user="app_user1", password="vault-secret"
        ) == TSV([["app_user1"]])
        assert "app_user1: Authentication failed" in instance.query_and_get_error(
            "SELECT currentUser()", user="app_user1", password="wrong"
        )
        assert ldap_user_count(instance, "app_user1") == "0"


def test_exclude_users_exposed_in_system_user_directories():
    # The names are stored in a set, so they come out sorted.
    assert query_as_admin(
        instance_a,
        "SELECT JSONExtract(params, 'exclude_users', 'Array(String)') "
        "FROM system.user_directories WHERE type = 'ldap'",
    ) == TSV([["['app_user1','janedoe']"]])


def test_interserver_query_keeps_mapped_roles(ldap_cluster):
    """
    The remote half of a query fanned out under the cluster secret authenticates the
    initial user on the receiving node with `AlwaysAllowCredentials`, which carry no LDAP
    role search results. The roles mapped at the user's last password login must survive
    that (https://github.com/ClickHouse/ClickHouse/pull/101920).
    """
    for instance in (instance_a, instance_b):
        query_as_admin(instance, "DROP ROLE IF EXISTS role_1")
        query_as_admin(instance, "CREATE ROLE role_1")
        query_as_admin(instance, "GRANT SELECT ON *.* TO role_1")
    setup_distributed_tables()
    add_ldap_group(ldap_cluster, group_cn="clickhouse-role_1", member_cn="johndoe")
    try:
        # Password logins map `role_1` on both nodes.
        for instance in (instance_a, instance_b):
            assert instance.query(
                "SELECT role_name FROM system.current_roles",
                user="johndoe",
                password="qwertz",
            ) == TSV([["role_1"]])
        assert query_as_admin(instance_b, "SHOW GRANTS FOR johndoe") == TSV(
            [["GRANT role_1 TO johndoe"]]
        )

        # instance_a authenticates johndoe against LDAP and forwards the remote part of
        # the query to instance_b under the cluster secret.
        assert (
            instance_a.query(
                "SELECT sum(id) FROM distributed_table",
                user="johndoe",
                password="qwertz",
            ).strip()
            == "6"
        )

        # Without the guard in `LDAPAccessStorage::authenticateImpl` the interserver
        # authentication on instance_b would have replaced johndoe's roles with the empty
        # set carried by `AlwaysAllowCredentials`.
        assert query_as_admin(instance_b, "SHOW GRANTS FOR johndoe") == TSV(
            [["GRANT role_1 TO johndoe"]]
        )
    finally:
        delete_ldap_group(ldap_cluster, group_cn="clickhouse-role_1")
        drop_distributed_tables()
        for instance in (instance_a, instance_b):
            query_as_admin(instance, "DROP ROLE IF EXISTS role_1")


def test_interserver_query_as_excluded_user_fails_on_remote_node():
    """
    `janedoe` exists only locally on instance_a. The remote half of the query reaches
    instance_b with `AlwaysAllowCredentials{janedoe}`; its LDAP directory must not
    materialise her from LDAP, and with no local definition there the remote
    authentication fails.
    """
    setup_distributed_tables()
    skip_lines_before = count_skip_log_lines(instance_b, "janedoe")
    try:
        assert "janedoe: Authentication failed" in instance_a.query_and_get_error(
            "SELECT sum(id) FROM distributed_table", user="janedoe", password="local"
        )
        assert count_skip_log_lines(instance_b, "janedoe") > skip_lines_before
        assert ldap_user_count(instance_b, "janedoe") == "0"
    finally:
        drop_distributed_tables()


def test_execute_as_excluded_user_resolves_to_local_definition():
    """
    `EXECUTE AS` resolves its target with `find(..., force_external_lookup = true)`, which
    lets the LDAP directory service-bind with `lookup_bind_dn` and materialise a user who has
    never logged in. On instance_a the excluded `janedoe` must come out as the local
    `users.xml` definition, and the LDAP directory must not have created her.
    """
    assert query_as_admin(instance_a, "EXECUTE AS janedoe SELECT currentUser()") == TSV(
        [["janedoe"]]
    )
    assert query_as_admin(
        instance_a, "SELECT storage FROM system.users WHERE name = 'janedoe'"
    ) == TSV([["users_xml"]])
    assert ldap_user_count(instance_a, "janedoe") == "0"


def test_execute_as_excluded_user_without_local_definition_fails_closed():
    """
    instance_b has no local `janedoe`, so the first, in-memory pass of the `EXECUTE AS`
    resolver misses everywhere and the forced pass reaches the LDAP directory. With
    `lookup_bind_dn` configured that pass would materialise her from LDAP; the exclusion must
    refuse the name before the server is contacted, leaving `UNKNOWN_USER`.
    """
    skip_lines_before = count_skip_log_lines(instance_b, "janedoe")
    error = instance_b.query_and_get_error(
        "EXECUTE AS janedoe SELECT currentUser()",
        user="common_user",
        password="qwerty",
    )
    assert "UNKNOWN_USER" in error, error
    assert "There is no user `janedoe`" in error, error
    # Nobody authenticates as janedoe here, so the only source of this line is the forced
    # overload of `findImpl`: the forced pass reached the directory and was refused there.
    assert count_skip_log_lines(instance_b, "janedoe") > skip_lines_before
    assert ldap_user_count(instance_b, "janedoe") == "0"


def test_execute_as_forced_lookup_materializes_non_excluded_ldap_user(ldap_cluster):
    """
    Positive control for the previous test: the same statement on the same node, for an LDAP
    user who is not excluded and has never logged in, does go through the service bind and
    materialises the user in the LDAP directory. So `janedoe` is refused because of
    `exclude_users` alone, not because the directory never looks anything up.
    """
    add_ldap_user(ldap_cluster, user_cn="ldap_only_user")
    try:
        assert query_as_admin(
            instance_b, "EXECUTE AS ldap_only_user SELECT currentUser()"
        ) == TSV([["ldap_only_user"]])
        assert ldap_user_count(instance_b, "ldap_only_user") == "1"
    finally:
        delete_ldap_user(ldap_cluster, user_cn="ldap_only_user")


def test_excluded_user_can_be_defined_locally_with_ldap_authentication():
    # Exclusion only concerns the directory. A user created with `IDENTIFIED WITH ldap`
    # lives in a later storage and still authenticates against the same LDAP server.
    query_as_admin(instance_b, "DROP USER IF EXISTS janedoe")
    query_as_admin(
        instance_b, "CREATE USER janedoe IDENTIFIED WITH ldap SERVER 'openldap'"
    )
    try:
        assert instance_b.query(
            "SELECT currentUser()", user="janedoe", password="qwerty"
        ) == TSV([["janedoe"]])
        assert query_as_admin(
            instance_b, "SELECT storage FROM system.users WHERE name = 'janedoe'"
        ) == TSV([["local_directory"]])
    finally:
        query_as_admin(instance_b, "DROP USER IF EXISTS janedoe")


def test_empty_exclude_users_entry_is_rejected_at_startup():
    # Must stay last: restarting instance_b discards its in-memory LDAP directory.
    instance_b.stop_clickhouse()
    instance_b.replace_in_config(
        LDAP_DIRECTORY_CONFIG_PATH, "<user>janedoe</user>", "<user></user>"
    )
    try:
        instance_b.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
        assert instance_b.get_process_pid("clickhouse") is None
        assert instance_b.contains_in_log("Empty user name in exclude_users")
    finally:
        instance_b.replace_in_config(
            LDAP_DIRECTORY_CONFIG_PATH, "<user></user>", "<user>janedoe</user>"
        )
        instance_b.start_clickhouse()
    assert instance_b.query(
        "SELECT currentUser()", user="johndoe", password="qwertz"
    ) == TSV([["johndoe"]])
