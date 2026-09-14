"""
Proactive synchronisation of an `ldap` user directory (`<sync>`).

The strict OpenLDAP fixture (tests/integration/compose/docker_compose_ldap_strict.yml,
bootstrapped by ci/docker/integration/runner/misc/openldap_strict/setup_strict.sh) refuses
anonymous binds, lets only the service account `cn=svc.clickhouse,ou=service,dc=example,dc=org`
read `ou=groups` and `memberOf`, has no size limit for that account (needed by the paged
enumeration) and runs the `memberof` overlay, which populates `memberOf` for groups added at
runtime under `ou=groups`. Every directory below enumerates the `inetOrgPerson` entries whose
`memberOf` names `clickhouse-role_a` or `clickhouse-role_b`, and maps the roles from the same
attribute (self-lookup: one paged search per run).

Nodes:
  - `node1`, `node2`: a cluster with a shared `replicated` access storage; the `ldap` directory
    is declared last, syncs every 2 s with `page_size` 100, creates `role_a`/`role_b` in
    `replicated` and excludes `johndoe`.
  - `node_dry`: same search with `dry_run`; nothing may change.
  - `node_bad`: wrong `lookup_password`, `interval` 0 (startup run + `SYSTEM RELOAD USERS`); also
    restarted with invalid configurations for the startup validation.
  - `node_stale`: the `ldap` directory FIRST (`interval` 1, `max_staleness` 3), then `users_xml`
    with the local user `local_after`: the gate order must keep local users reachable while the
    directory is stale.
  - `node_mem`: a `memory` directory before `replicated` and no `roles_storage`: role creation must
    refuse the ephemeral pick.

Group `clickhouse-role_a`/`clickhouse-role_b` always keep the service account as a member
(`groupOfNames` requires one), which is not an `inetOrgPerson` and therefore never synchronised.
`permanent` is a member of `clickhouse-role_a` for the whole module so that removing `janedoe`
never trips `min_users`.
"""

import logging
import os
import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from helpers.test_tools import TSV, assert_eq_with_retry, assert_logs_contain_with_retry

LDAP_HOST = "openldap_strict"
LDAP_PORT = 1389
LDAP_SUFFIX = "dc=example,dc=org"
USERS_CONTAINER = f"ou=users,{LDAP_SUFFIX}"
GROUPS_CONTAINER = f"ou=groups,{LDAP_SUFFIX}"
SERVICE_CONTAINER = f"ou=service,{LDAP_SUFFIX}"
LDAP_ADMIN_BIND_DN = f"cn=admin,{LDAP_SUFFIX}"
LDAP_ADMIN_PASSWORD = "clickhouse"
LDAP_SERVICE_BIND_DN = f"cn=svc.clickhouse,{SERVICE_CONTAINER}"
LDAP_SERVICE_PASSWORD = "svcsecret"
LDAP_SERVER_NAME = "openldap_strict"
LOOKUP_BIND_FAILED = (
    f"LDAP lookup bind as '{LDAP_SERVICE_BIND_DN}' failed for server '{LDAP_SERVER_NAME}':"
    " invalid credentials"
)

ROLE_A_GROUP = "clickhouse-role_a"
ROLE_B_GROUP = "clickhouse-role_b"
BULK_USERS = 1200

DOCKER_COMPOSE_PATH = get_docker_compose_path()
CONFIGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs")
CONFIG_D = "/etc/clickhouse-server/config.d"
ERR_LOG = "clickhouse-server.err.log"

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/ldap_server.xml",
        "configs/directories_cluster.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/ldap_server.xml",
        "configs/directories_cluster.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node_dry = cluster.add_instance(
    "node_dry",
    main_configs=["configs/ldap_server.xml", "configs/directories_dry.xml"],
    user_configs=["configs/users.xml"],
)

node_bad = cluster.add_instance(
    "node_bad",
    main_configs=["configs/ldap_server_bad_lookup.xml", "configs/directories_bad.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

node_stale = cluster.add_instance(
    "node_stale",
    main_configs=["configs/ldap_server.xml", "configs/directories_stale.xml"],
    user_configs=["configs/users_stale.xml"],
    stay_alive=True,
)

node_mem = cluster.add_instance(
    "node_mem",
    main_configs=["configs/ldap_server.xml", "configs/directories_mem.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)


def user_dn(cn):
    return f"cn={cn},{USERS_CONTAINER}"


def group_dn(cn):
    return f"cn={cn},{GROUPS_CONTAINER}"


# ---------------------------------------------------------------------------------------------
# LDAP helpers: everything runs inside the `openldap_strict` container as the directory admin.
# ---------------------------------------------------------------------------------------------


def ldap_exec(command, ignore_codes=()):
    """Run a shell command inside the LDAP container; `ignore_codes` are exit codes of the last
    ldap* tool that count as success (e.g. 20 "Type or value exists" for an idempotent add).
    """
    if ignore_codes:
        alternatives = " || ".join(f"[ $? -eq {code} ]" for code in ignore_codes)
        command = f"{command} || {alternatives}"
    return cluster.exec_in_container(
        cluster.get_instance_docker_id(LDAP_HOST), ["bash", "-c", command], user="root"
    )


def ldap_tool(tool, extra=""):
    return (
        f"{tool} -x -H ldap://localhost:{LDAP_PORT} -D {shlex.quote(LDAP_ADMIN_BIND_DN)}"
        f" -w {LDAP_ADMIN_PASSWORD} {extra}"
    )


def ldap_add(ldif, ignore_codes=()):
    ldap_exec(
        f"printf '%s\\n' {shlex.quote(ldif)} | {ldap_tool('ldapadd')}", ignore_codes
    )


def ldap_modify(ldif, ignore_codes=()):
    ldap_exec(
        f"printf '%s\\n' {shlex.quote(ldif)} | {ldap_tool('ldapmodify')}", ignore_codes
    )


def ldap_delete(dn, ignore_missing=False):
    # 32 = "No such object".
    ldap_exec(ldap_tool("ldapdelete", shlex.quote(dn)), (32,) if ignore_missing else ())


def ldap_add_user(cn, uid=None, password="qwerty", container=USERS_CONTAINER):
    ldap_add(
        f"dn: cn={cn},{container}\n"
        "objectClass: inetOrgPerson\n"
        f"cn: {cn}\n"
        "sn: Test\n"
        f"uid: {uid or cn}\n"
        f"userPassword: {password}\n",
        ignore_codes=(68,),  # "Already exists"
    )


def ldap_add_group(group_cn, member_dns=()):
    """Create a `groupOfNames` under `ou=groups` (readable by the service account only); the
    service account itself is always a member so that the group can be emptied of users.
    """
    members = "".join(f"member: {dn}\n" for dn in [LDAP_SERVICE_BIND_DN, *member_dns])
    ldap_add(
        f"dn: {group_dn(group_cn)}\n"
        "objectClass: top\n"
        "objectClass: groupOfNames\n"
        f"cn: {group_cn}\n"
        f"{members}",
        ignore_codes=(68,),
    )


def ldap_set_member(group_cn, member_dn, present):
    # 20 = "Type or value exists" (already a member), 16 = "No such attribute" (not a member).
    operation = "add" if present else "delete"
    ldap_modify(
        f"dn: {group_dn(group_cn)}\n"
        "changetype: modify\n"
        f"{operation}: member\n"
        f"member: {member_dn}\n",
        ignore_codes=(20,) if present else (16,),
    )


def ldap_set_memberships(cn, groups):
    """Make user `cn` a member of exactly the given groups among role_a/role_b."""
    for group_cn in (ROLE_A_GROUP, ROLE_B_GROUP):
        ldap_set_member(group_cn, user_dn(cn), group_cn in groups)


def bulk_user_cn(i):
    return f"bulk{i:04d}"


def ldap_add_bulk_users(count):
    """Generate the LDIF inside the container: a payload of that size does not fit into a single
    `docker exec` argument."""
    entry = (
        f"dn: cn=bulk%04d,{USERS_CONTAINER}\\n"
        "objectClass: inetOrgPerson\\n"
        "cn: bulk%04d\\n"
        "sn: Bulk\\n"
        "uid: bulk%04d\\n"
        "userPassword: qwerty\\n\\n"
    )
    ldap_exec(
        f"for i in $(seq 1 {count}); do printf '{entry}' $i $i $i; done > /tmp/bulk_users.ldif"
        f" && {ldap_tool('ldapadd', '-f /tmp/bulk_users.ldif')}"
    )


def ldap_set_bulk_membership(group_cn, count):
    ldap_exec(
        f"printf 'dn: {group_dn(group_cn)}\\nchangetype: modify\\nadd: member\\n' > /tmp/bulk_members.ldif"
        f" && for i in $(seq 1 {count}); do printf 'member: cn=bulk%04d,{USERS_CONTAINER}\\n' $i; done"
        " >> /tmp/bulk_members.ldif"
        f" && {ldap_tool('ldapmodify', '-f /tmp/bulk_members.ldif')}"
    )


def ldap_delete_bulk_users(count):
    ldap_exec(
        f"for i in $(seq 1 {count}); do printf 'cn=bulk%04d,{USERS_CONTAINER}\\n' $i; done"
        f" > /tmp/bulk_dns.txt && {ldap_tool('ldapdelete', '-f /tmp/bulk_dns.txt')}"
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
                f" -b {USERS_CONTAINER} '(uid=janedoe)' dn"
                f" | grep -c '^dn: {user_dn('janedoe')}$'"
                " | grep 1 >> /dev/null"
            )
            logging.info("openldap_strict is ready")
            return
        except Exception as ex:
            if attempts % 10 == 0:
                logging.info(
                    "openldap_strict not ready after %s attempts: %s", attempts, str(ex)
                )
            time.sleep(1)
    raise Exception("Timed out waiting for openldap_strict")


# ---------------------------------------------------------------------------------------------
# ClickHouse helpers.
# ---------------------------------------------------------------------------------------------


def admin(node, sql):
    return node.query(sql, user="admin")


def admin_error(node, sql):
    return node.query_and_get_error(sql, user="admin")


def ldap_users_query(name=None):
    where = f"AND name = '{name}'" if name else ""
    return f"SELECT count() FROM system.users WHERE storage = 'ldap' {where}"


def granted_roles_query(user_name):
    return (
        "SELECT granted_role_name FROM system.role_grants"
        f" WHERE user_name = '{user_name}' ORDER BY granted_role_name"
    )


def wait_ldap_user(node, name, present, **kwargs):
    assert_eq_with_retry(
        node, ldap_users_query(name), "1" if present else "0", user="admin", **kwargs
    )


def wait_granted_roles(node, user_name, roles, **kwargs):
    assert_eq_with_retry(
        node,
        granted_roles_query(user_name),
        TSV([[role] for role in roles]),
        user="admin",
        **kwargs,
    )


def event_value(node, event):
    return int(
        admin(
            node, f"SELECT sum(value) FROM system.events WHERE event = '{event}'"
        ).strip()
    )


def login(node, user, password="qwerty"):
    return node.query("SELECT currentUser()", user=user, password=password)


def login_error(node, user, password="qwerty"):
    error = node.query_and_get_error(
        "SELECT currentUser()", user=user, password=password
    )
    assert "Authentication failed" in error, error
    return error


def count_in_log(node, substring):
    return int(node.count_in_log(substring).strip() or 0)


def read_config(name):
    with open(os.path.join(CONFIGS_DIR, name)) as f:
        return f.read()


def reload_config(node, config_name, content):
    node.replace_config(f"{CONFIG_D}/{config_name}", content)
    admin(node, "SYSTEM RELOAD CONFIG")


def sync_section(**overrides):
    values = {
        "interval": "0",
        "base_dn": LDAP_SUFFIX,
        "scope": "subtree",
        "search_filter": (
            "(&amp;(objectClass=inetOrgPerson)"
            f"(|(memberOf={group_dn(ROLE_A_GROUP)})(memberOf={group_dn(ROLE_B_GROUP)})))"
        ),
        "attribute": "uid",
    }
    values.update(overrides)
    return "".join(f"<{key}>{value}</{key}>" for key, value in values.items())


def directories_bad_config(**sync_overrides):
    """`directories_bad.xml` with the `<sync>` section replaced."""
    original = read_config("directories_bad.xml")
    start = original.index("<sync>")
    end = original.index("</sync>") + len("</sync>")
    return (
        original[:start]
        + f"<sync>{sync_section(**sync_overrides)}</sync>"
        + original[end:]
    )


# ---------------------------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    docker_compose_ldap_strict = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_ldap_strict.yml"
    )
    try:
        cluster.start()

        # The strict LDAP server is started outside of `cluster.start`, like in
        # test_ldap_search_and_bind, so it does not disturb the shared fixture. Until it is up,
        # every synchronisation fails and is retried; nothing else depends on that window.
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

        ldap_add_user("permanent")
        # `johndoe` is excluded on node1/node2 and must never appear although he is in both groups.
        ldap_add_group(
            ROLE_A_GROUP, [user_dn("permanent"), user_dn("janedoe"), user_dn("johndoe")]
        )
        ldap_add_group(ROLE_B_GROUP, [user_dn("johndoe")])

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


@pytest.fixture
def janedoe_in_role_a():
    """The steady state of the module: `janedoe` in `clickhouse-role_a` only, synchronised on both
    cluster nodes. Restored after each test that changes her memberships."""
    ldap_set_memberships("janedoe", {ROLE_A_GROUP})
    for node in (node1, node2):
        wait_granted_roles(node, "janedoe", ["role_a"])
    try:
        yield
    finally:
        ldap_set_memberships("janedoe", {ROLE_A_GROUP})
        for node in (node1, node2):
            wait_granted_roles(node, "janedoe", ["role_a"])


# ---------------------------------------------------------------------------------------------
# Cluster: roles, users, grants, propagation.
# ---------------------------------------------------------------------------------------------


def test_roles_are_created_once_in_the_pinned_storage():
    for node in (node1, node2):
        assert_eq_with_retry(
            node,
            "SELECT name, storage FROM system.roles WHERE name IN ('role_a', 'role_b') ORDER BY name",
            TSV([["role_a", "replicated"], ["role_b", "replicated"]]),
            user="admin",
        )

    # `tryInsert` into the shared `replicated` storage: exactly one node wins the race per role.
    for role in ("role_a", "role_b"):
        created = sum(
            count_in_log(node, f"Created role '{role}' in storage")
            for node in (node1, node2)
        )
        assert created == 1, (role, created)
    assert not node1.contains_in_log("cannot create roles")


def test_users_are_materialised_with_their_roles_before_any_login(janedoe_in_role_a):
    for node in (node1, node2):
        assert admin(
            node,
            "SELECT name, storage, auth_type FROM system.users WHERE name = 'janedoe'",
        ) == TSV([["janedoe", "ldap", "ldap"]])
        assert admin(node, granted_roles_query("permanent")) == TSV([["role_a"]])
        assert node.contains_in_log("Added LDAP user 'janedoe'")
    # The password is still checked against the directory.
    assert login(node2, "janedoe") == TSV([["janedoe"]])
    login_error(node2, "janedoe", "wrong")


def test_grants_on_a_synced_role_apply_on_every_node(janedoe_in_role_a):
    admin(node1, "GRANT SELECT ON default.* TO role_a")
    admin(node2, "DROP TABLE IF EXISTS grants_table SYNC")
    admin(node2, "CREATE TABLE grants_table (id UInt32) ENGINE = MergeTree ORDER BY id")
    admin(node2, "INSERT INTO grants_table VALUES (1), (2)")
    try:
        # The grant is made on node1 and lives in `replicated`; janedoe never logged in on node2.
        assert_eq_with_retry(
            node2,
            "SELECT count() FROM system.grants WHERE role_name = 'role_a'"
            " AND access_type = 'SELECT' AND database = 'default'",
            "1",
            user="admin",
        )
        assert node2.query(
            "SELECT count() FROM grants_table", user="janedoe", password="qwerty"
        ) == TSV([["2"]])
    finally:
        admin(node2, "DROP TABLE IF EXISTS grants_table SYNC")


def test_role_changes_propagate_on_reload_on_cluster(janedoe_in_role_a):
    ldap_set_member(ROLE_B_GROUP, user_dn("janedoe"), True)
    admin(node1, "SYSTEM RELOAD USERS ON CLUSTER test_ldap_cluster")
    for node in (node1, node2):
        assert admin(node, granted_roles_query("janedoe")) == TSV(
            [["role_a"], ["role_b"]]
        )

    ldap_set_member(ROLE_A_GROUP, user_dn("janedoe"), False)
    admin(node1, "SYSTEM RELOAD USERS ON CLUSTER test_ldap_cluster")
    for node in (node1, node2):
        assert admin(node, granted_roles_query("janedoe")) == TSV([["role_b"]])
        assert node.contains_in_log("Updated roles of LDAP user 'janedoe'")


def test_cached_login_cannot_regrant_a_revoked_role(janedoe_in_role_a):
    """`verification_cooldown` is 300 s. In a synced directory a login verifies the password only,
    so the cached role set of an earlier login can never be re-applied over the synchronisation.
    """
    ldap_set_member(ROLE_B_GROUP, user_dn("janedoe"), True)
    admin(node1, "SYSTEM RELOAD USERS")
    assert node1.query(
        "SELECT role_name FROM system.current_roles ORDER BY role_name",
        user="janedoe",
        password="qwerty",
    ) == TSV([["role_a"], ["role_b"]])

    ldap_set_member(ROLE_B_GROUP, user_dn("janedoe"), False)
    admin(node1, "SYSTEM RELOAD USERS")

    # Within the cooldown: the password check is answered from the cache, the roles are not.
    assert node1.query(
        "SELECT role_name FROM system.current_roles ORDER BY role_name",
        user="janedoe",
        password="qwerty",
    ) == TSV([["role_a"]])
    assert admin(node1, granted_roles_query("janedoe")) == TSV([["role_a"]])


def test_offboarding_removes_the_user_without_a_login(janedoe_in_role_a):
    ldap_set_memberships("janedoe", set())
    for node in (node1, node2):
        wait_ldap_user(node, "janedoe", present=False)
        assert node.contains_in_log("Removed LDAP user 'janedoe'")
        # Cached password verification or not, the name is no longer in the snapshot.
        login_error(node, "janedoe")
    # `permanent` keeps the directory above `min_users`.
    assert admin(node1, ldap_users_query("permanent")) == "1\n"


def test_excluded_user_is_never_synchronised():
    # `johndoe` is a member of both groups and listed in `exclude_users`.
    admin(node1, "SYSTEM RELOAD USERS")
    for node in (node1, node2):
        assert admin(node, ldap_users_query("johndoe")) == "0\n"
        assert event_value(node, "LDAPSyncUsersExcluded") > 0
    login_error(node1, "johndoe", "qwertz")


def test_user_of_a_preceding_storage_is_not_synchronised(janedoe_in_role_a):
    """`replicated` precedes `ldap`: a name it defines wins at login time, so the synchronisation
    must not materialise it. Once the local user is dropped the next run materialises the entry.
    """
    admin(node1, "CREATE USER janedoe IDENTIFIED WITH plaintext_password BY 'x'")
    try:
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                "SELECT storage FROM system.users WHERE name = 'janedoe' ORDER BY storage",
                TSV([["replicated"]]),
                user="admin",
            )
            assert_logs_contain_with_retry(
                node,
                "LDAP user 'janedoe' (cn=janedoe,ou=users,dc=example,dc=org) exists in storage .replicated., which precedes directory .ldap.: not synchronised",
            )
            assert event_value(node, "LDAPSyncUsersShadowed") > 0
        assert login(node1, "janedoe", "x") == TSV([["janedoe"]])
    finally:
        admin(node1, "DROP USER IF EXISTS janedoe")

    for node in (node1, node2):
        assert_eq_with_retry(
            node,
            "SELECT storage FROM system.users WHERE name = 'janedoe' ORDER BY storage",
            TSV([["ldap"]]),
            user="admin",
        )


def test_distributed_query_reaches_a_node_the_user_never_logged_in_on(
    janedoe_in_role_a,
):
    admin(node1, "GRANT SELECT ON default.* TO role_a")
    for node in (node1, node2):
        admin(node, "DROP TABLE IF EXISTS local_table SYNC")
        admin(
            node, "CREATE TABLE local_table (id UInt32) ENGINE = MergeTree ORDER BY id"
        )
    admin(node2, "INSERT INTO local_table VALUES (1), (2), (3)")
    admin(node1, "DROP TABLE IF EXISTS distributed_table SYNC")
    admin(
        node1,
        "CREATE TABLE distributed_table AS local_table"
        " ENGINE = Distributed(test_ldap_cluster, default, local_table)",
    )
    try:
        # The remote half reaches node2 under the cluster secret with `AlwaysAllowCredentials`:
        # janedoe is in node2's snapshot, so it is accepted, and her roles are left alone.
        assert (
            node1.query(
                "SELECT sum(id) FROM distributed_table",
                user="janedoe",
                password="qwerty",
            ).strip()
            == "6"
        )
        assert admin(node2, granted_roles_query("janedoe")) == TSV([["role_a"]])
    finally:
        admin(node1, "DROP TABLE IF EXISTS distributed_table SYNC")
        for node in (node1, node2):
            admin(node, "DROP TABLE IF EXISTS local_table SYNC")


def test_dropped_role_is_recreated_and_granted_again(janedoe_in_role_a):
    admin(node1, "DROP ROLE role_a")
    # The next run creates the allow-listed role again and `processRoleChange` re-grants it.
    for node in (node1, node2):
        assert_eq_with_retry(
            node,
            "SELECT storage FROM system.roles WHERE name = 'role_a'",
            "replicated",
            user="admin",
        )
        wait_granted_roles(node, "janedoe", ["role_a"])


def test_user_ids_are_stable_across_reloads(janedoe_in_role_a):
    id_before = admin(node1, "SELECT id FROM system.users WHERE name = 'janedoe'")
    admin(node1, "SYSTEM RELOAD USERS")
    admin(node1, "SYSTEM RELOAD USERS")
    assert (
        admin(node1, "SELECT id FROM system.users WHERE name = 'janedoe'") == id_before
    )


def test_params_expose_the_sync_section_without_secrets():
    params = admin(
        node1, "SELECT params FROM system.user_directories WHERE type = 'ldap'"
    )
    assert '"sync":' in params, params
    assert '"interval":2' in params, params
    assert '"page_size":100' in params, params
    assert '"roles_storage":"replicated"' in params, params
    assert '"only_synced_users":true' in params, params
    assert LDAP_SERVICE_PASSWORD not in params, params
    assert "lookup" not in params, params


# ---------------------------------------------------------------------------------------------
# Guards.
# ---------------------------------------------------------------------------------------------


def test_min_users_guard(janedoe_in_role_a):
    """With every user out of both groups the run finds nobody: it is refused and applies
    nothing, so the synchronised users stay."""
    ldap_set_memberships("janedoe", set())
    ldap_set_memberships("permanent", set())
    try:
        error = admin_error(node1, "SYSTEM RELOAD USERS")
        assert "found 0 users" in error and "fewer than min_users = 1" in error, error
        assert admin(node1, ldap_users_query("permanent")) == "1\n"
        assert admin(node1, ldap_users_query("janedoe")) == "1\n"
    finally:
        ldap_set_memberships("permanent", {ROLE_A_GROUP})


def test_duplicate_user_name_guard(janedoe_in_role_a):
    """`uid=dupuser` exists under `ou=users` and `ou=service`; with both in a group the
    enumeration returns two entries for one login and the run is refused."""
    for container in (USERS_CONTAINER, SERVICE_CONTAINER):
        ldap_set_member(ROLE_A_GROUP, f"cn=dupuser,{container}", True)
    try:
        error = admin_error(node1, "SYSTEM RELOAD USERS")
        assert "share the user name 'dupuser' (ambiguous directory)" in error, error
        assert admin(node1, ldap_users_query("dupuser")) == "0\n"
    finally:
        for container in (USERS_CONTAINER, SERVICE_CONTAINER):
            ldap_set_member(ROLE_A_GROUP, f"cn=dupuser,{container}", False)
    admin(node1, "SYSTEM RELOAD USERS")


def test_paged_enumeration_and_mass_removal_guard(janedoe_in_role_a):
    """1200 users in `clickhouse-role_b` need 13 pages of 100 and exceed the default OpenLDAP
    size limit of 500, which the fixture lifts for the service account. Deleting the group would
    remove them all at once: `max_removed_fraction` refuses the run. Deleting the users afterwards
    trips the same guard on purpose; the nodes are restarted to start from an empty directory,
    which also covers the restart window."""
    base = int(admin(node1, ldap_users_query()).strip())
    ldap_add_bulk_users(BULK_USERS)
    ldap_set_bulk_membership(ROLE_B_GROUP, BULK_USERS)
    try:
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                ldap_users_query(),
                str(base + BULK_USERS),
                user="admin",
                retry_count=60,
            )
        assert admin(node1, granted_roles_query(bulk_user_cn(1200))) == TSV(
            [["role_b"]]
        )
        assert login(node2, bulk_user_cn(777)) == TSV([[bulk_user_cn(777)]])

        ldap_delete(group_dn(ROLE_B_GROUP))
        error = admin_error(node1, "SYSTEM RELOAD USERS")
        assert f"would remove {BULK_USERS} of {base + BULK_USERS} users" in error, error
        assert "max_removed_fraction = 0.5" in error, error
        assert admin(node1, ldap_users_query()) == f"{base + BULK_USERS}\n"
        assert event_value(node1, "LDAPSyncFailures") > 0

        # Restoring the group makes the next run succeed again (nothing to change).
        ldap_add_group(ROLE_B_GROUP, [user_dn("johndoe")])
        ldap_set_bulk_membership(ROLE_B_GROUP, BULK_USERS)
        admin(node1, "SYSTEM RELOAD USERS")
        assert admin(node1, ldap_users_query()) == f"{base + BULK_USERS}\n"
    finally:
        ldap_delete(group_dn(ROLE_B_GROUP), ignore_missing=True)
        ldap_delete_bulk_users(BULK_USERS)
        ldap_add_group(ROLE_B_GROUP, [user_dn("johndoe")])

    # Every run now wants to remove 1200 of 1202 users and is refused. A restart starts from an
    # empty directory: nobody logs in through it until the first run, then the users are back.
    for node in (node1, node2):
        node.restart_clickhouse()
    for node in (node1, node2):
        assert_eq_with_retry(
            node, ldap_users_query(), str(base), user="admin", retry_count=60
        )
        wait_granted_roles(node, "janedoe", ["role_a"])
        assert login(node, "janedoe") == TSV([["janedoe"]])


# ---------------------------------------------------------------------------------------------
# Single-node variants.
# ---------------------------------------------------------------------------------------------


def test_dry_run_changes_nothing(janedoe_in_role_a):
    assert_logs_contain_with_retry(
        node_dry,
        "Dry run: would add LDAP user 'janedoe' (cn=janedoe,ou=users,dc=example,dc=org)",
    )
    assert_logs_contain_with_retry(
        node_dry, "Dry run: would create role 'role_a' in storage .local_directory."
    )
    assert node_dry.contains_in_log("(dry run) finished")
    assert admin(node_dry, ldap_users_query()) == "0\n"
    assert admin(node_dry, "SELECT count() FROM system.roles") == "0\n"
    assert not node_dry.contains_in_log("Added LDAP user")
    assert not node_dry.contains_in_log("Created role")
    login_error(node_dry, "janedoe")


def test_wrong_lookup_password_fails_the_run():
    # The startup run (`interval` 0) happened before the LDAP fixture was up and failed on the
    # connection; the forced run fails on the service account bind, which is an operator error.
    error = admin_error(node_bad, "SYSTEM RELOAD USERS")
    assert "LDAP lookup bind as" in error and "invalid credentials" in error, error
    assert node_bad.contains_in_log(LOOKUP_BIND_FAILED)
    assert node_bad.contains_in_log("LDAP synchronisation of directory .ldap. failed")
    assert event_value(node_bad, "LDAPSyncFailures") >= 2
    assert event_value(node_bad, "LDAPSyncRuns") >= 2
    assert admin(node_bad, ldap_users_query()) == "0\n"


def test_ephemeral_roles_storage_is_refused():
    assert_logs_contain_with_retry(
        node_mem,
        "LDAP sync cannot create roles in ephemeral storage .memory.; set roles_storage",
    )
    error = admin_error(node_mem, "SYSTEM RELOAD USERS")
    assert "cannot create roles in ephemeral storage `memory`" in error, error
    assert (
        admin(node_mem, "SELECT count() FROM system.roles WHERE storage = 'memory'")
        == "0\n"
    )
    assert admin(node_mem, "SELECT count() FROM system.roles") == "0\n"
    assert admin(node_mem, ldap_users_query()) == "0\n"


def test_staleness_gate_refuses_synced_users_only(janedoe_in_role_a):
    """`node_stale`: `ldap` (interval 1, max_staleness 3) is declared before `users_xml`. While
    the directory cannot be synchronised, janedoe is refused with the staleness error, but the
    local user that follows and unknown names get the ordinary "not found" treatment, and
    `SYSTEM RELOAD USERS` returns the LDAP error while still reloading the other storages.
    """
    wait_ldap_user(node_stale, "janedoe", present=True)
    assert login(node_stale, "janedoe") == TSV([["janedoe"]])
    assert login(node_stale, "local_after", "local") == TSV([["local_after"]])

    original_config = read_config("ldap_server.xml")
    broken_config = original_config.replace(
        f"<lookup_password>{LDAP_SERVICE_PASSWORD}</lookup_password>",
        "<lookup_password>wrong</lookup_password>",
    )
    assert broken_config != original_config
    try:
        reload_config(node_stale, "ldap_server.xml", broken_config)

        # The runs fail every second from now on; after 3 s the snapshot counts as stale.
        deadline = time.time() + 30
        while True:
            error = login_error(node_stale, "janedoe")
            if node_stale.contains_in_log("has not been synchronised for"):
                break
            assert time.time() < deadline, error
            time.sleep(0.5)
        assert node_stale.contains_in_log("refusing to authenticate user 'janedoe'")

        # Gate order: the local user behind the directory and an unknown name are unaffected.
        assert login(node_stale, "local_after", "local") == TSV([["local_after"]])
        login_error(node_stale, "nosuchuser")
        assert not node_stale.contains_in_log(
            "refusing to authenticate user 'nosuchuser'"
        )
        assert not node_stale.contains_in_log(
            "refusing to authenticate user 'local_after'"
        )

        # Reload propagation: the failing `ldap` directory comes first, the error is returned, and
        # `users_xml` was still reloaded.
        error = admin_error(node_stale, "SYSTEM RELOAD USERS")
        assert "LDAP lookup bind as" in error, error
        assert node_stale.contains_in_log("Failed to reload access storage .ldap.")
        assert not node_stale.contains_in_log(
            "Failed to reload access storage .users_xml."
        )
    finally:
        reload_config(node_stale, "ldap_server.xml", original_config)

    admin(node_stale, "SYSTEM RELOAD USERS")
    assert login(node_stale, "janedoe") == TSV([["janedoe"]])


# ---------------------------------------------------------------------------------------------
# Startup validation and role storage selection (restarts of `node_bad`; keep these last).
# ---------------------------------------------------------------------------------------------


def restart_node_bad_with(
    directories_config, server_config=None, expected_to_fail=False
):
    node_bad.stop_clickhouse()
    node_bad.replace_config(f"{CONFIG_D}/directories_bad.xml", directories_config)
    if server_config is not None:
        node_bad.replace_config(f"{CONFIG_D}/ldap_server_bad_lookup.xml", server_config)
    node_bad.start_clickhouse(expected_to_fail=expected_to_fail)


def restore_node_bad():
    restart_node_bad_with(
        read_config("directories_bad.xml"), read_config("ldap_server_bad_lookup.xml")
    )


def assert_startup_fails_with(directories_config, expected_message):
    try:
        restart_node_bad_with(directories_config, expected_to_fail=True)
        assert (
            node_bad.grep_in_log(expected_message, filename=ERR_LOG, only_latest=True)
            != ""
        ), expected_message
    finally:
        restore_node_bad()


def test_nonexistent_roles_storage_fails_the_first_run(janedoe_in_role_a):
    try:
        restart_node_bad_with(
            directories_bad_config(create_roles="true", roles_storage="nonexistent"),
            server_config=read_config("ldap_server.xml"),
        )
        assert_logs_contain_with_retry(
            node_bad, "Access storage with name nonexistent is not found"
        )
        error = admin_error(node_bad, "SYSTEM RELOAD USERS")
        assert "Access storage with name nonexistent is not found" in error, error
        assert admin(node_bad, ldap_users_query()) == "0\n"
        assert admin(node_bad, "SELECT count() FROM system.roles") == "0\n"
    finally:
        restore_node_bad()


def test_startup_validation_of_max_staleness():
    assert_startup_fails_with(
        directories_bad_config(max_staleness="3", interval="0"),
        "'max_staleness' in 'user_directories.ldap.sync' section requires a periodic synchronisation ('interval' > 0)",
    )
    assert_startup_fails_with(
        directories_bad_config(max_staleness="3", interval="5"),
        "'max_staleness' (3 s) in 'user_directories.ldap.sync' section must be greater than 'interval' (5 s)",
    )
    assert_startup_fails_with(
        directories_bad_config(
            max_staleness="3", interval="1", only_synced_users="false"
        ),
        "'max_staleness' in 'user_directories.ldap.sync' section requires 'only_synced_users' = true",
    )


def test_startup_validation_of_the_other_sync_keys():
    assert_startup_fails_with(
        directories_bad_config(page_size="0"),
        "'page_size' in 'user_directories.ldap.sync' section must be between 1 and 1000, got 0",
    )
    assert_startup_fails_with(
        directories_bad_config(max_users="1", min_users="1"),
        "'max_users' in 'user_directories.ldap.sync' section must be 0 (unlimited) or greater than 'min_users' (1), got 1",
    )
    assert_startup_fails_with(
        directories_bad_config(max_removed_fraction="1.5"),
        "'max_removed_fraction' in 'user_directories.ldap.sync' section must be between 0 and 1, got 1.5",
    )
    assert_startup_fails_with(
        directories_bad_config(attribute="dn"),
        "'attribute' in 'user_directories.ldap.sync' section must name the attribute that holds the user name",
    )
    assert_startup_fails_with(
        directories_bad_config(search_filter="(uid={user_name})"),
        "'base_dn' and 'search_filter' in 'user_directories.ldap.sync' section must not contain '{user_name}'",
    )
    assert_startup_fails_with(
        directories_bad_config(max_user="5"),
        "Unknown entry 'max_user' in 'user_directories.ldap.sync' section",
    )
    # `create_roles` without any `groups` allow-list: nothing would be safe to create.
    without_groups = directories_bad_config(create_roles="true")
    start = without_groups.index("<groups>")
    end = without_groups.index("</groups>") + len("</groups>")
    without_groups = without_groups[:start] + without_groups[end:]
    assert_startup_fails_with(
        without_groups,
        "'create_roles' in 'user_directories.ldap.sync' section requires a non-empty 'groups' allow-list",
    )
