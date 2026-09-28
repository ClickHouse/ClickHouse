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
    restarted with invalid configurations for the startup validation, among them a second `ldap`
    directory next to the synchronised one, which must be the only `ldap` directory of the server.
  - `node_stale`: the `ldap` directory FIRST (`interval` 1, `max_staleness` 3), then `users_xml`
    with the local user `local_after`: the gate order must keep local users reachable while the
    directory is stale.
  - `node_mem`: a `memory` directory before `replicated` and no `roles_storage`: role creation must
    refuse the ephemeral pick.
  - `node_manual`: `interval` 0 (startup run + `SYSTEM RELOAD USERS`), otherwise the search of
    `node1` with the roles in `local_directory`. The guard tests read their exact numbers from it: a
    directory that synchronises only when told to cannot pick up a half-applied LDAP change between
    two steps of a test, which a periodic node does (see `wait_ldap_synced_entries`).

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

from helpers.fake_ldap import start_fake_ldap_server
from helpers.client import QueryRuntimeException
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
USERS_D = "/etc/clickhouse-server/users.d"
ERR_LOG = "clickhouse-server.err.log"

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/sync_logger.xml",
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
        "configs/sync_logger.xml",
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
    main_configs=[
        "configs/sync_logger.xml",
        "configs/ldap_server.xml",
        "configs/directories_dry.xml",
    ],
    user_configs=["configs/users.xml"],
)

node_bad = cluster.add_instance(
    "node_bad",
    main_configs=[
        "configs/sync_logger.xml",
        "configs/ldap_server_bad_lookup.xml",
        "configs/directories_bad.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

node_stale = cluster.add_instance(
    "node_stale",
    main_configs=[
        "configs/sync_logger.xml",
        "configs/ldap_server.xml",
        "configs/directories_stale.xml",
        "configs/remote_servers.xml",
    ],
    user_configs=["configs/users_stale.xml"],
    stay_alive=True,
)

node_mem = cluster.add_instance(
    "node_mem",
    main_configs=[
        "configs/sync_logger.xml",
        "configs/ldap_server.xml",
        "configs/directories_mem.xml",
    ],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)

node_manual = cluster.add_instance(
    "node_manual",
    main_configs=[
        "configs/sync_logger.xml",
        "configs/ldap_server.xml",
        "configs/directories_manual.xml",
    ],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


def user_dn(cn):
    return f"cn={cn},{USERS_CONTAINER}"


def group_dn(cn):
    return f"cn={cn},{GROUPS_CONTAINER}"


# The `<sync>` search of every directory in this module (`&` is escaped in the XML configurations).
SYNC_SEARCH_FILTER = (
    "(&(objectClass=inetOrgPerson)"
    f"(|(memberOf={group_dn(ROLE_A_GROUP)})(memberOf={group_dn(ROLE_B_GROUP)})))"
)


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
    # `-c` carries on past entries that do not exist (32 = "No such object"): the cleanup must
    # work whatever step of the test failed.
    ldap_exec(
        f"for i in $(seq 1 {count}); do printf 'cn=bulk%04d,{USERS_CONTAINER}\\n' $i; done"
        f" > /tmp/bulk_dns.txt && {ldap_tool('ldapdelete', '-c -f /tmp/bulk_dns.txt')}",
        ignore_codes=(32,),
    )


def ldap_count_synced_entries():
    """Number of entries the `<sync>` search returns right now, read as the service account: only
    it may read `memberOf`, and it has no size limit, so a plain search lists every match.
    """
    output = ldap_exec(
        f"/opt/bitnami/openldap/bin/ldapsearch -LLL -x -H ldap://localhost:{LDAP_PORT}"
        f" -D {shlex.quote(LDAP_SERVICE_BIND_DN)} -w {LDAP_SERVICE_PASSWORD}"
        f" -b {LDAP_SUFFIX} -s sub {shlex.quote(SYNC_SEARCH_FILTER)} dn"
    )
    return sum(1 for line in output.splitlines() if line.startswith("dn:"))


def wait_ldap_synced_entries(expected, timeout=60):
    """Wait until the `<sync>` search matches exactly `expected` entries.

    The `memberof` overlay turns a bulk `add: member` or the deletion of a group into one internal
    modification per member, each visible to concurrent searches as soon as it commits, so a search
    that runs while such an operation is in progress sees a partial state (a periodic node applied
    "107 removed" out of 1200 in CI, and the forced run that followed reported "1093 of 1095"
    instead of "1200 of 1202"). Waiting for the directory itself, before forcing a run, is what
    makes the numbers of that run exact. There is no harness helper for a condition on the LDAP
    side (`assert_eq_with_retry` and `wait_for_log_line` observe a ClickHouse node), hence the
    explicit bounded loop."""
    deadline = time.time() + timeout
    while True:
        count = ldap_count_synced_entries()
        if count == expected:
            return
        assert (
            time.time() < deadline
        ), f"the sync search matches {count} entries, expected {expected}"
        time.sleep(0.5)


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


def sync_node_manual():
    """`node_manual` synchronises on `SYSTEM RELOAD USERS` only: bring it to the current state
    of the directory and return the number of users it holds."""
    admin(node_manual, "SYSTEM RELOAD USERS")
    return int(admin(node_manual, ldap_users_query()).strip())


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
        "search_filter": SYNC_SEARCH_FILTER.replace("&", "&amp;"),
        "attribute": "uid",
    }
    values.update(overrides)
    return "".join(f"<{key}>{value}</{key}>" for key, value in values.items())


def directories_bad_config(role_mapping=None, after_ldap=None, **sync_overrides):
    """`directories_bad.xml` with the `<sync>` section replaced; optionally the content of the
    `<role_mapping>` section replaced by `role_mapping` and a storage `after_ldap` inserted after the
    `ldap` directory."""
    original = read_config("directories_bad.xml")
    start = original.index("<sync>")
    end = original.index("</sync>") + len("</sync>")
    result = (
        original[:start]
        + f"<sync>{sync_section(**sync_overrides)}</sync>"
        + original[end:]
    )
    if role_mapping is not None:
        start = result.index("<role_mapping>")
        end = result.index("</role_mapping>") + len("</role_mapping>")
        result = (
            result[:start]
            + f"<role_mapping>{role_mapping}</role_mapping>"
            + result[end:]
        )
    if after_ldap is not None:
        anchor = "        </ldap>\n"
        assert result.count(anchor) == 1
        result = result.replace(anchor, anchor + after_ldap)
    return result


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
        # `auth_type` is an array: a user may carry several authentication methods.
        assert admin(
            node,
            "SELECT name, storage, auth_type FROM system.users WHERE name = 'janedoe'",
        ) == TSV([["janedoe", "ldap", "['ldap']"]])
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
# Guards. Exercised on `node_manual`: with `interval` 0 the only runs are the forced ones, so what
# a refused run reports cannot depend on whether a periodic run slipped in between two LDAP changes
# of a test (and applied one of them). The periodic nodes see the same changes and converge on
# their own; the fixtures wait for that.
# ---------------------------------------------------------------------------------------------


def test_min_users_guard(janedoe_in_role_a):
    """With every user out of both groups the run finds nobody: it is refused and applies
    nothing, so the synchronised users stay."""
    sync_node_manual()
    ldap_set_memberships("janedoe", set())
    ldap_set_memberships("permanent", set())
    try:
        error = admin_error(node_manual, "SYSTEM RELOAD USERS")
        assert "found 0 users" in error and "fewer than min_users = 1" in error, error
        assert admin(node_manual, ldap_users_query("permanent")) == "1\n"
        assert admin(node_manual, ldap_users_query("janedoe")) == "1\n"
    finally:
        ldap_set_memberships("permanent", {ROLE_A_GROUP})


def test_duplicate_user_name_guard(janedoe_in_role_a):
    """`uid=dupuser` exists under `ou=users` and `ou=service`; with both in a group the
    enumeration returns two entries for one login and the run is refused."""
    sync_node_manual()
    for container in (USERS_CONTAINER, SERVICE_CONTAINER):
        ldap_set_member(ROLE_A_GROUP, f"cn=dupuser,{container}", True)
    try:
        error = admin_error(node_manual, "SYSTEM RELOAD USERS")
        assert "share the user name 'dupuser' (ambiguous directory)" in error, error
        assert admin(node_manual, ldap_users_query("dupuser")) == "0\n"
    finally:
        for container in (USERS_CONTAINER, SERVICE_CONTAINER):
            ldap_set_member(ROLE_A_GROUP, f"cn=dupuser,{container}", False)
    # Nothing ambiguous is left: the next run succeeds again.
    admin(node_manual, "SYSTEM RELOAD USERS")


def test_entry_with_several_user_names_fails_the_run(janedoe_in_role_a):
    """`uid` is multi-valued in `inetOrgPerson`. An entry that matches the search but does not yield
    exactly one user name fails the run instead of being skipped: skipped, it would be a user missing
    from the plan, whom the next run would remove as if they had left the directory."""
    base_users = sync_node_manual()
    base_entries = ldap_count_synced_entries()
    ldap_add(
        f"dn: {user_dn('twonames')}\n"
        "objectClass: inetOrgPerson\n"
        "cn: twonames\n"
        "sn: Test\n"
        "uid: twonames\n"
        "uid: twonames2\n"
        "userPassword: qwerty\n",
        ignore_codes=(68,),  # "Already exists"
    )
    try:
        ldap_set_member(ROLE_A_GROUP, user_dn("twonames"), True)
        wait_ldap_synced_entries(base_entries + 1)

        failures_before = event_value(node_manual, "LDAPSyncFailures")
        error = admin_error(node_manual, "SYSTEM RELOAD USERS")
        assert (
            f"LDAP entry '{user_dn('twonames')}' returned by the user enumeration on server"
            f" '{LDAP_SERVER_NAME}' has 2 values of the user name attribute 'uid', expected exactly one"
        ) in error, error
        assert event_value(node_manual, "LDAPSyncFailures") == failures_before + 1
        # A failed run changes nothing: neither name was materialised, nobody was removed.
        assert admin(node_manual, ldap_users_query()) == f"{base_users}\n"
        for name in ("twonames", "twonames2"):
            assert admin(node_manual, ldap_users_query(name)) == "0\n"
    finally:
        ldap_set_member(ROLE_A_GROUP, user_dn("twonames"), False)
        ldap_delete(user_dn("twonames"), ignore_missing=True)
        wait_ldap_synced_entries(base_entries)
    # The directory is well-formed again: the next run succeeds with nothing to change.
    assert sync_node_manual() == base_users


def test_paged_enumeration_and_mass_removal_guard(janedoe_in_role_a):
    """1200 users in `clickhouse-role_b` need 13 pages of 100 and exceed the default OpenLDAP
    size limit of 500, which the fixture lifts for the service account. Deleting the group would
    remove them all at once: `max_removed_fraction` refuses the run.

    The exact numbers are read from `node_manual`, and every forced run is preceded by a wait for
    the directory itself (see `wait_ldap_synced_entries`). The periodic nodes see the same changes
    in whatever slices their runs happen to observe and are only required to converge. The cleanup
    leaves them holding more bulk users than `max_removed_fraction` lets one run remove, so they
    are restarted to start from an empty directory, which also covers the restart window.
    """
    base_users = sync_node_manual()
    # `johndoe` matches the search but is excluded: one entry more than users.
    base_entries = ldap_count_synced_entries()
    assert base_entries == base_users + 1, (base_entries, base_users)
    total_users = base_users + BULK_USERS
    total_entries = base_entries + BULK_USERS
    try:
        ldap_add_bulk_users(BULK_USERS)
        ldap_set_bulk_membership(ROLE_B_GROUP, BULK_USERS)
        wait_ldap_synced_entries(total_entries)

        admin(node_manual, "SYSTEM RELOAD USERS")
        assert admin(node_manual, ldap_users_query()) == f"{total_users}\n"
        assert node_manual.contains_in_log(
            f"Received page 13 of the LDAP search under '{LDAP_SUFFIX}' on server"
            f" '{LDAP_SERVER_NAME}': {total_entries} entries so far"
        )
        assert node_manual.contains_in_log(
            f": {total_entries} entries, {total_users} users ({BULK_USERS} added, 0 updated,"
            " 0 removed, 1 excluded, 0 shadowed), 0 roles created, 0 roles missing"
        )
        assert admin(node_manual, granted_roles_query(bulk_user_cn(1200))) == TSV(
            [["role_b"]]
        )
        assert login(node_manual, bulk_user_cn(777)) == TSV([[bulk_user_cn(777)]])
        # The periodic nodes get there in as many runs as it takes them.
        for node in (node1, node2):
            assert_eq_with_retry(
                node,
                ldap_users_query(),
                str(total_users),
                user="admin",
                retry_count=120,
            )

        ldap_delete(group_dn(ROLE_B_GROUP))
        wait_ldap_synced_entries(base_entries)
        failures_before = event_value(node_manual, "LDAPSyncFailures")
        error = admin_error(node_manual, "SYSTEM RELOAD USERS")
        assert f"would remove {BULK_USERS} of {total_users} users" in error, error
        assert "max_removed_fraction = 0.5" in error, error
        assert admin(node_manual, ldap_users_query()) == f"{total_users}\n"
        assert event_value(node_manual, "LDAPSyncFailures") > failures_before

        # Restoring the group makes the next run succeed again, with nothing to change.
        ldap_add_group(ROLE_B_GROUP, [user_dn("johndoe")])
        ldap_set_bulk_membership(ROLE_B_GROUP, BULK_USERS)
        wait_ldap_synced_entries(total_entries)
        admin(node_manual, "SYSTEM RELOAD USERS")
        assert admin(node_manual, ldap_users_query()) == f"{total_users}\n"
        assert node_manual.contains_in_log(
            f": {total_entries} entries, {total_users} users (0 added, 0 updated, 0 removed,"
            " 1 excluded, 0 shadowed), 0 roles created, 0 roles missing"
        )
    finally:
        # Everything below runs whatever assertion failed, so that the tests that follow never
        # start with a fixture full of bulk users or with a node that refuses every run.
        ldap_delete(group_dn(ROLE_B_GROUP), ignore_missing=True)
        # Let the overlay finish dropping `memberOf` from the bulk users before deleting them.
        wait_ldap_synced_entries(base_entries)
        ldap_delete_bulk_users(BULK_USERS)
        ldap_add_group(ROLE_B_GROUP, [user_dn("johndoe")])

        # A periodic node that applied the additions still holds the bulk users and now sees more
        # than `max_removed_fraction` of them gone: it refuses every run and, with `max_staleness`,
        # turns stale (`node_stale`). A restart starts from an empty directory: nobody logs in
        # through it until the first run, then the base users are back. `node_manual` holds the
        # bulk users as well and is restarted for the same reason.
        for node in (node1, node2, node_stale, node_manual):
            node.restart_clickhouse()
        for node in (node1, node2):
            assert_eq_with_retry(
                node, ldap_users_query(), str(base_users), user="admin", retry_count=60
            )
            wait_granted_roles(node, "janedoe", ["role_a"])
            assert login(node, "janedoe") == TSV([["janedoe"]])
        assert sync_node_manual() == base_users
        # `node_stale` excludes nobody and syncs every second; it must be fresh again for the
        # staleness test below.
        wait_ldap_user(node_stale, "janedoe", present=True, retry_count=60)
        assert login(node_stale, "janedoe") == TSV([["janedoe"]])


# ---------------------------------------------------------------------------------------------
# Single-node variants.
# ---------------------------------------------------------------------------------------------


def test_role_in_another_storage_is_not_created_again(janedoe_in_role_a):
    """`create_roles` has the global `CREATE ROLE IF NOT EXISTS` semantics: a role of the name in any
    storage, not only in `roles_storage`, is left alone, so the name keeps resolving to one role.
    `node_manual` has a `memory` storage after `local_directory` for this."""
    sync_node_manual()
    created_before = count_in_log(node_manual, "Created role 'role_b'")
    admin(node_manual, "DROP ROLE role_b")
    admin(node_manual, "CREATE ROLE role_b IN memory")
    try:
        admin(node_manual, "SYSTEM RELOAD USERS")
        assert (
            admin(node_manual, "SELECT storage FROM system.roles WHERE name = 'role_b'")
            == "memory\n"
        )
        assert count_in_log(node_manual, "Created role 'role_b'") == created_before
    finally:
        admin(node_manual, "DROP ROLE IF EXISTS role_b")
        admin(node_manual, "SYSTEM RELOAD USERS")
        assert (
            admin(node_manual, "SELECT storage FROM system.roles WHERE name = 'role_b'")
            == "local_directory\n"
        )


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


def test_execute_as_resolves_synced_users_only(janedoe_in_role_a):
    """`EXECUTE AS` resolves its target through the forced lookup of the directory, which applies the
    login gates: a synchronised user is impersonated with the roles of the last run; a name outside
    the snapshot is unknown, although the directory could resolve it, and nothing is materialised
    (a lazy directory would look the name up and create the user here)."""
    sync_node_manual()
    assert admin(node_manual, "EXECUTE AS janedoe SELECT currentUser()") == TSV(
        [["janedoe"]]
    )
    assert admin(
        node_manual,
        "EXECUTE AS janedoe SELECT role_name FROM system.current_roles ORDER BY role_name",
    ) == TSV([["role_a"]])

    # `lazyonly` exists in the directory but is in no group, so the snapshot does not have them.
    ldap_add_user("lazyonly")
    try:
        error = admin_error(node_manual, "EXECUTE AS lazyonly SELECT currentUser()")
        assert "UNKNOWN_USER" in error, error
        assert admin(node_manual, ldap_users_query("lazyonly")) == "0\n"
        assert node_manual.contains_in_log(
            "User lazyonly is not in the synchronised snapshot of directory .ldap."
        )
    finally:
        ldap_delete(user_dn("lazyonly"), ignore_missing=True)


def test_removed_user_is_dropped_from_row_policies_like_drop_user(janedoe_in_role_a):
    """A row policy `TO janedoe` lives in `local_directory` and names the user by id. The removal by the
    synchronisation strips that reference exactly as `DROP USER` does; left in place, it would point at a
    dead id, and when janedoe rejoins the group and is materialised under a new id the policy would silently
    not apply to her. Now it visibly does not: the table has row policies, none of them for the new janedoe
    (`throw_on_unmatched_row_policies` is set by the harness), and `apply_to_list` no longer names her.

    The discriminating check is the stored definition of the policy: `apply_to_list` and `SHOW CREATE` are
    rendered by `RolesOrUsersSet::toASTWithNames`, which silently drops an id it cannot resolve to a name,
    and the new janedoe is denied with or without the cleanup, so all three look the same with a dangling
    id. `local_directory` rewrites `<policy id>.sql` synchronously on every update, in the attach form that
    names the users by id (`TO ID('<uuid>')`), so the file shows whether the reference is really gone.
    """
    sync_node_manual()
    policy_query = (
        "SELECT apply_to_all, apply_to_list FROM system.row_policies"
        " WHERE short_name = 'sync_policy'"
    )
    admin(node_manual, "DROP TABLE IF EXISTS policy_table SYNC")
    admin(
        node_manual,
        "CREATE TABLE policy_table (id UInt32) ENGINE = MergeTree ORDER BY id",
    )
    try:
        admin(node_manual, "INSERT INTO policy_table VALUES (1), (2), (3)")
        admin(node_manual, "GRANT SELECT ON default.policy_table TO role_a")
        admin(
            node_manual,
            "CREATE ROW POLICY sync_policy ON default.policy_table FOR SELECT USING id < 2 TO janedoe",
        )
        assert admin(node_manual, policy_query) == TSV([["0", "['janedoe']"]])
        assert node_manual.query(
            "SELECT count() FROM policy_table", user="janedoe", password="qwerty"
        ) == TSV([["1"]])
        id_before = admin(
            node_manual, "SELECT id FROM system.users WHERE name = 'janedoe'"
        )
        janedoe_id = id_before.strip()
        policy_id = admin(
            node_manual,
            "SELECT id FROM system.row_policies WHERE short_name = 'sync_policy'",
        ).strip()
        policy_file = f"/var/lib/clickhouse/access/{policy_id}.sql"

        def stored_policy_definition():
            return node_manual.exec_in_container(["bash", "-c", f"cat {policy_file}"])

        definition = stored_policy_definition()
        assert f"ID('{janedoe_id}')" in definition, definition

        ldap_set_memberships("janedoe", set())
        admin(node_manual, "SYSTEM RELOAD USERS")
        assert admin(node_manual, ldap_users_query("janedoe")) == "0\n"
        assert node_manual.contains_in_log("Removed LDAP user 'janedoe'")
        # With the cleanup reverted, the file would still hold the old id while the two renderings below
        # would look exactly the same.
        definition = stored_policy_definition()
        assert janedoe_id not in definition and "ID(" not in definition, definition
        assert admin(node_manual, policy_query) == TSV([["0", "[]"]])
        assert "janedoe" not in admin(
            node_manual, "SHOW CREATE ROW POLICY sync_policy ON default.policy_table"
        )

        ldap_set_memberships("janedoe", {ROLE_A_GROUP})
        admin(node_manual, "SYSTEM RELOAD USERS")
        assert admin(node_manual, ldap_users_query("janedoe")) == "1\n"
        assert (
            admin(node_manual, "SELECT id FROM system.users WHERE name = 'janedoe'")
            != id_before
        )
        assert admin(node_manual, policy_query) == TSV([["0", "[]"]])
        assert "ID(" not in stored_policy_definition()
        error = node_manual.query_and_get_error(
            "SELECT count() FROM policy_table", user="janedoe", password="qwerty"
        )
        assert (
            "Table default.policy_table has row policies, but none of them are for the current user"
            in error
        ), error
    finally:
        ldap_set_memberships("janedoe", {ROLE_A_GROUP})
        admin(
            node_manual,
            "DROP ROW POLICY IF EXISTS sync_policy ON default.policy_table",
        )
        admin(node_manual, "DROP TABLE IF EXISTS policy_table SYNC")
        admin(node_manual, "REVOKE SELECT ON default.policy_table FROM role_a")


def test_staleness_gate_refuses_synced_users_only(janedoe_in_role_a):
    """`node_stale`: `ldap` (interval 1, max_staleness 3) is declared before `users_xml`. While
    the directory cannot be synchronised, janedoe is refused with the staleness error, for a login
    and for `EXECUTE AS` alike, but the local user that follows and unknown names get the ordinary
    "not found" treatment, and `SYSTEM RELOAD USERS` returns the LDAP error while still reloading
    the other storages.
    """
    # The precondition is a fresh snapshot: `node_stale` syncs every second, so a directory that
    # a previous test left refusing its runs fails here, not in the middle of the scenario.
    wait_ldap_user(node_stale, "janedoe", present=True, retry_count=60)
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

        # The runs fail every second from now on; after 3 s the snapshot counts as stale. Until then
        # a login may still succeed (or fail on the lookup bind, whose cache the reload cleared).
        # `AccessControl::authenticate` hides every reason behind "Authentication failed"; the
        # staleness error is visible in the server log only.
        deadline = time.time() + 30
        while not node_stale.contains_in_log("has not been synchronised for"):
            assert time.time() < deadline, "the directory did not become stale in time"
            try:
                login(node_stale, "janedoe")
            except QueryRuntimeException:
                pass
            time.sleep(0.5)
        login_error(node_stale, "janedoe")
        assert node_stale.contains_in_log("refusing to authenticate user 'janedoe'")

        # `EXECUTE AS` goes through the same gate; the lookup is not hidden behind the generic
        # authentication error, so the reason reaches the client.
        error = admin_error(node_stale, "EXECUTE AS janedoe SELECT currentUser()")
        assert "LDAP_ERROR" in error and "has not been synchronised for" in error, error
        assert "refusing to resolve user 'janedoe'" in error, error

        # The interserver path goes through the same gate: a fanout from node1 under the cluster
        # secret reaches node_stale as janedoe (`AlwaysAllowCredentials`), the second shard, which is
        # stale and refuses her. The query needs enough to build and run the cluster function on node1,
        # where she is fresh; the exact error the initiator surfaces varies, so node_stale's log is the
        # signal that the gate ran on the interserver path.
        admin(node1, "GRANT SELECT, SHOW COLUMNS, REMOTE ON *.* TO role_a")
        try:
            refusals = count_in_log(
                node_stale, "refusing to authenticate user 'janedoe'"
            )
            node1.query_and_get_error(
                "SELECT count() FROM clusterAllReplicas('test_ldap_cluster_stale', system.one)",
                user="janedoe",
                password="qwerty",
            )
            assert (
                count_in_log(node_stale, "refusing to authenticate user 'janedoe'")
                > refusals
            )
        finally:
            admin(node1, "REVOKE SELECT, SHOW COLUMNS, REMOTE ON *.* FROM role_a")

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
        # `users_xml`, declared after it, was still reloaded: a user added to its file right before
        # the command is there afterwards. (The periodic reload of `users_xml` would pick the file up
        # within a couple of seconds as well; the command runs right after the write.)
        users_config = read_config("users_stale.xml")
        users_config_with_one_more = users_config.replace(
            "    </users>",
            "        <local_reloaded>\n            <password>reloaded</password>\n"
            "        </local_reloaded>\n    </users>",
        )
        assert users_config_with_one_more != users_config
        node_stale.replace_config(
            f"{USERS_D}/users_stale.xml", users_config_with_one_more
        )
        error = admin_error(node_stale, "SYSTEM RELOAD USERS")
        assert "LDAP lookup bind as" in error, error
        assert node_stale.contains_in_log("Failed to reload access storage .ldap.")
        assert not node_stale.contains_in_log(
            "Failed to reload access storage .users_xml."
        )
        assert login(node_stale, "local_reloaded", "reloaded") == TSV(
            [["local_reloaded"]]
        )
        assert (
            admin(
                node_stale,
                "SELECT storage FROM system.users WHERE name = 'local_reloaded'",
            )
            == "users_xml\n"
        )
    finally:
        node_stale.replace_config(
            f"{USERS_D}/users_stale.xml", read_config("users_stale.xml")
        )
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


def test_synced_user_shadows_a_same_named_user_of_a_later_storage(janedoe_in_role_a):
    """`node_stale` declares `users_xml` after the synchronised directory, and `users_xml` defines
    `shadowed` with the password `local`. Once the directory materialises an LDAP user of that name,
    the LDAP entry takes precedence: the LDAP password logs in through the directory, and the local
    password is an authentication failure that stops the chain, not a "not found" that would let
    `users_xml` authenticate the same login name. Once the LDAP user is gone, the local one is
    reachable again."""
    assert login(node_stale, "shadowed", "local") == TSV([["shadowed"]])
    ldap_add_user("shadowed")
    try:
        ldap_set_memberships("shadowed", {ROLE_A_GROUP})
        wait_ldap_user(node_stale, "shadowed", present=True, retry_count=60)

        assert login(node_stale, "shadowed") == TSV([["shadowed"]])
        login_error(node_stale, "shadowed", "local")
        failed = node_stale.grep_in_log("user: shadowed: Authentication failed")
        assert "Invalid credentials" in failed, failed
    finally:
        ldap_set_memberships("shadowed", set())
        ldap_delete(user_dn("shadowed"), ignore_missing=True)

    wait_ldap_user(node_stale, "shadowed", present=False, retry_count=60)
    assert login(node_stale, "shadowed", "local") == TSV([["shadowed"]])


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


def test_max_users_guard(janedoe_in_role_a):
    """`max_users` bounds the enumeration itself: the run stops at the first entry beyond the limit,
    before the plan, and applies nothing: the users of the earlier runs stay, and no role is created,
    not even one that was dropped. `node_bad` is restarted with the limit set to the live count
    (`permanent`, `janedoe` and `johndoe`, whom it does not exclude) and with the roles created in
    `local_directory`; one more member of `clickhouse-role_a` takes the search over the limit.
    """
    live_entries = ldap_count_synced_entries()
    try:
        restart_node_bad_with(
            directories_bad_config(
                max_users=str(live_entries),
                create_roles="true",
                roles_storage="local_directory",
            ),
            server_config=read_config("ldap_server.xml"),
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, ldap_users_query()) == f"{live_entries}\n"
        assert (
            admin(
                node_bad,
                "SELECT count() FROM system.roles WHERE name IN ('role_a', 'role_b')",
            )
            == "2\n"
        )

        ldap_add_user("overflow")
        ldap_set_memberships("overflow", {ROLE_A_GROUP})
        wait_ldap_synced_entries(live_entries + 1)
        admin(node_bad, "DROP ROLE role_b")
        failures_before = event_value(node_bad, "LDAPSyncFailures")
        error = admin_error(node_bad, "SYSTEM RELOAD USERS")
        assert (
            f"returned more than {live_entries} entries; refusing to continue" in error
        ), error
        assert admin(node_bad, ldap_users_query()) == f"{live_entries}\n"
        assert admin(node_bad, ldap_users_query("overflow")) == "0\n"
        assert (
            admin(node_bad, "SELECT count() FROM system.roles WHERE name = 'role_b'")
            == "0\n"
        )
        assert event_value(node_bad, "LDAPSyncFailures") > failures_before

        # Back under the limit, the next run succeeds and creates the dropped role again.
        ldap_set_memberships("overflow", set())
        ldap_delete(user_dn("overflow"))
        wait_ldap_synced_entries(live_entries)
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, ldap_users_query()) == f"{live_entries}\n"
        assert (
            admin(node_bad, "SELECT count() FROM system.roles WHERE name = 'role_b'")
            == "1\n"
        )
    finally:
        ldap_set_memberships("overflow", set())
        ldap_delete(user_dn("overflow"), ignore_missing=True)
        restore_node_bad()
        # The roles live in `local_directory` and survive the restart; the other tests of `node_bad` start from none.
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


LDAP_SERVER_WITHOUT_LOOKUP = """<clickhouse>
    <ldap_servers>
        <openldap_strict>
            <host>openldap_strict</host>
            <port>1389</port>
            <enable_tls>no</enable_tls>
            <bind_dn>cn={user_name},ou=users,dc=example,dc=org</bind_dn>
        </openldap_strict>
    </ldap_servers>
</clickhouse>
"""

SYNC_SERVER_REFUSED = "LDAP sync requires 'lookup_bind_dn' on server 'openldap_strict'"


def ldap_server_config_duplicated():
    """`ldap_server.xml` with its `<openldap_strict>` block twice: a section-level error, recorded for
    the name by `ExternalAuthenticators::setConfiguration`, that parsing one block alone would miss.
    """
    original = read_config("ldap_server.xml")
    start = original.index("        <openldap_strict>")
    end = original.index("        </openldap_strict>") + len(
        "        </openldap_strict>\n"
    )
    return original[:end] + original[start:end] + original[end:]


def test_sync_server_must_have_a_lookup_identity():
    """The enumeration binds as the lookup identity of the server. A synchronised directory whose server
    has none would only find out at its first run, with nobody able to log in through it meanwhile
    (`only_synced_users`). The servers are known once the storages exist: at startup the check fails the
    start, on the final parse state of `ldap_servers` (a duplicated name counts), and a
    `SYSTEM RELOAD CONFIG` that would take the lookup identity away is checked before it is applied, so
    it fails and leaves the previous servers in effect."""
    try:
        restart_node_bad_with(
            directories_bad_config(),
            server_config=LDAP_SERVER_WITHOUT_LOOKUP,
            expected_to_fail=True,
        )
        for expected in (
            SYNC_SERVER_REFUSED,
            "while checking LDAP server 'openldap_strict' of user directory .ldap., which has a 'sync' section",
        ):
            assert (
                node_bad.grep_in_log(expected, filename=ERR_LOG, only_latest=True) != ""
            ), expected

        restart_node_bad_with(
            directories_bad_config(), server_config=read_config("ldap_server.xml")
        )
        restart_node_bad_with(
            directories_bad_config(),
            server_config=ldap_server_config_duplicated(),
            expected_to_fail=True,
        )
        assert (
            node_bad.grep_in_log(
                "LDAP server 'openldap_strict' is misconfigured: Multiple LDAP servers with the same name are not allowed",
                filename=ERR_LOG,
                only_latest=True,
            )
            != ""
        )

        # The two cases above left node_bad stopped (expected_to_fail); bring it back with a good
        # server so the reload-config case below starts from a running node.
        restart_node_bad_with(
            directories_bad_config(), server_config=read_config("ldap_server.xml")
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        node_bad.replace_config(
            f"{CONFIG_D}/ldap_server_bad_lookup.xml", LDAP_SERVER_WITHOUT_LOOKUP
        )
        error = admin_error(node_bad, "SYSTEM RELOAD CONFIG")
        assert SYNC_SERVER_REFUSED in error, error
        # The previous server definition is still the one in effect.
        admin(node_bad, "SYSTEM RELOAD USERS")
    finally:
        restore_node_bad()


def test_only_synced_users_false_serves_lazy_logins(janedoe_in_role_a):
    """With `only_synced_users` false the directory keeps its lazy path for names outside the snapshot.
    `lazylogin` is in no group, so the search never returns them, yet the password login works and
    materialises them here, with no roles; a run leaves them alone (it removes only the users it
    materialised itself). Once a group returns them, a run grants the roles and owns the user from
    then on, so leaving the group removes them like any synchronised user."""
    ldap_add_user("lazylogin")
    try:
        restart_node_bad_with(
            directories_bad_config(
                only_synced_users="false",
                create_roles="true",
                roles_storage="local_directory",
            ),
            server_config=read_config("ldap_server.xml"),
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        base_users = int(admin(node_bad, ldap_users_query()).strip())
        assert admin(node_bad, ldap_users_query("lazylogin")) == "0\n"

        assert login(node_bad, "lazylogin") == TSV([["lazylogin"]])
        assert admin(node_bad, ldap_users_query("lazylogin")) == "1\n"
        assert (
            node_bad.query(
                "SELECT count() FROM system.current_roles",
                user="lazylogin",
                password="qwerty",
            )
            == "0\n"
        )

        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, ldap_users_query("lazylogin")) == "1\n"
        assert admin(node_bad, ldap_users_query()) == f"{base_users + 1}\n"

        ldap_set_memberships("lazylogin", {ROLE_A_GROUP})
        wait_ldap_synced_entries(base_users + 1)
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, granted_roles_query("lazylogin")) == TSV([["role_a"]])

        ldap_set_memberships("lazylogin", set())
        wait_ldap_synced_entries(base_users)
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, ldap_users_query("lazylogin")) == "0\n"
    finally:
        ldap_set_memberships("lazylogin", set())
        ldap_delete(user_dn("lazylogin"), ignore_missing=True)
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


LDAP_SERVER_STALLED_SEARCH = """<clickhouse>
    <ldap_servers>
        <openldap_strict>
            <!-- `fake_ldap_server.py` in `search` mode inside the node: it answers every bind with
                 success and never a search, so only `search_timeout` can end an enumeration. -->
            <host>127.0.0.1</host>
            <port>3892</port>
            <enable_tls>no</enable_tls>
            <lookup_bind_dn>cn=svc.clickhouse,ou=service,dc=example,dc=org</lookup_bind_dn>
            <lookup_password>svcsecret</lookup_password>
            <bind_dn>{user_dn}</bind_dn>
            <user_dn_detection>
                <base_dn>dc=example,dc=org</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=inetOrgPerson)(uid={user_name}))</search_filter>
            </user_dn_detection>
            <operation_timeout>20</operation_timeout>
            <network_timeout>20</network_timeout>
            <search_timeout>1</search_timeout>
        </openldap_strict>
    </ldap_servers>
</clickhouse>
"""


def test_stalled_page_fails_the_run_within_search_timeout(janedoe_in_role_a):
    """The paged enumeration (`LDAPSyncClient::enumerate`, `searchEntries`) is bounded by
    `search_timeout` like the ordinary searches: the fake server answers the service bind and never
    the search that follows, so the run can only end through that timeout (1 second here, the other
    two timeouts are 20 seconds), fails with it and applies nothing, the snapshot of the previous
    run stays."""
    try:
        restart_node_bad_with(
            directories_bad_config(), server_config=read_config("ldap_server.xml")
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        base_users = int(admin(node_bad, ldap_users_query()).strip())
        assert base_users > 0

        start_fake_ldap_server(node_bad, 3892, "search")
        node_bad.replace_config(
            f"{CONFIG_D}/ldap_server_bad_lookup.xml", LDAP_SERVER_STALLED_SEARCH
        )
        admin(node_bad, "SYSTEM RELOAD CONFIG")

        failures_before = event_value(node_bad, "LDAPSyncFailures")
        start = time.monotonic()
        error = admin_error(node_bad, "SYSTEM RELOAD USERS")
        elapsed = time.monotonic() - start
        assert "Timed out" in error, error
        assert elapsed < 15, f"the run took {elapsed:.1f}s"
        assert admin(node_bad, ldap_users_query()) == f"{base_users}\n"
        assert event_value(node_bad, "LDAPSyncFailures") == failures_before + 1
    finally:
        restore_node_bad()


ROLE_MAPPING_BY_BIND_DN = (
    "<base_dn>ou=groups,dc=example,dc=org</base_dn><scope>subtree</scope>"
    "<search_filter>(member={bind_dn})</search_filter><attribute>cn</attribute>"
    "<groups><group>clickhouse-role_a</group><group>clickhouse-role_b</group></groups>"
    "<prefix>clickhouse-</prefix>"
)


def test_bind_dn_placeholder_of_the_synchronisation_matches_a_login(janedoe_in_role_a):
    """With a `bind_dn` template next to `lookup_bind_dn` (direct bind, searches under the service
    account), a login substitutes `{bind_dn}` with the template's result and `{user_dn}` with the
    detected DN. The run does the same, so a mapping over `{bind_dn}` grants what a login would: the
    template yields `uid=janedoe,ou=users,...`, which is not janedoe's entry (`cn=janedoe,...`) but
    is what `clickhouse-role_b` lists as a member here, and only that role is granted; substituting
    the entry's DN would grant `role_a` instead."""
    template_dn = f"uid=janedoe,{USERS_CONTAINER}"
    ldap_set_member(ROLE_B_GROUP, template_dn, True)
    try:
        server_config = read_config("ldap_server.xml").replace(
            "<bind_dn>{user_dn}</bind_dn>",
            f"<bind_dn>uid={{user_name}},{USERS_CONTAINER}</bind_dn>",
        )
        assert "{user_dn}</bind_dn>" not in server_config
        restart_node_bad_with(
            directories_bad_config(
                role_mapping=ROLE_MAPPING_BY_BIND_DN,
                create_roles="true",
                roles_storage="local_directory",
            ),
            server_config=server_config,
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, granted_roles_query("janedoe")) == TSV([["role_b"]])
    finally:
        ldap_set_member(ROLE_B_GROUP, template_dn, False)
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


LOCAL_DIRECTORY_AFTER = (
    "        <local_directory>\n"
    "            <name>local_directory_after</name>\n"
    "            <path>/var/lib/clickhouse/access_after/</path>\n"
    "        </local_directory>\n"
)


def test_roles_are_not_created_ahead_of_a_storage_reloaded_after_the_directory(
    janedoe_in_role_a,
):
    """`SYSTEM RELOAD USERS` reloads the other storages first and the synchronised directory last,
    so that its run sees a role a later storage loads in the same command instead of creating a
    copy, which would leave two roles of the name. `local_directory_after` is declared after the
    directory and gets a role file written by hand, the way a restored backup or a file copied from
    another node appears."""
    role_id = "11111111-2222-3333-4444-555555555555"
    try:
        restart_node_bad_with(
            directories_bad_config(
                after_ldap=LOCAL_DIRECTORY_AFTER,
                create_roles="true",
                roles_storage="local_directory",
            ),
            server_config=read_config("ldap_server.xml"),
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert (
            admin(node_bad, "SELECT storage FROM system.roles WHERE name = 'role_b'")
            == "local_directory\n"
        )

        admin(node_bad, "DROP ROLE role_b")
        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                f"echo 'ATTACH ROLE role_b;' > /var/lib/clickhouse/access_after/{role_id}.sql",
            ],
            user="root",
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(
            node_bad, "SELECT id, storage FROM system.roles WHERE name = 'role_b'"
        ) == TSV([[role_id, "local_directory_after"]])
    finally:
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")
        node_bad.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/access_after"], user="root"
        )
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


LOCAL_DIRECTORY_BEFORE = (
    "        <local_directory>\n"
    "            <name>local_directory_before</name>\n"
    "            <path>/var/lib/clickhouse/access_before/</path>\n"
    "        </local_directory>\n"
)


def role_grant_ids(node, user_name, role_name):
    return admin(
        node,
        f"SELECT granted_role_id FROM system.role_grants WHERE user_name = '{user_name}' AND granted_role_name = '{role_name}'",
    )


def test_synced_users_hold_the_role_the_name_resolves_to(janedoe_in_role_a):
    """A role of a granted name published by another storage after the run created its copy must not
    split the role: the users of the directory hold exactly the role the name resolves to, the first
    storage in `user_directories` order. `local_directory_after` (after the directory) and
    `local_directory_before` (before `local_directory`, where the roles are created) each get a role
    file written by hand: the copy in the later storage is shadowed and changes nothing, the copy in
    the earlier storage takes over `johndoe`'s grant, and the run warns about the duplicate.
    """
    id_after = "22222222-2222-2222-2222-222222222222"
    id_before = "33333333-3333-3333-3333-333333333333"
    try:
        config = directories_bad_config(
            after_ldap=LOCAL_DIRECTORY_AFTER,
            create_roles="true",
            roles_storage="local_directory",
        )
        anchor = "        <local_directory>\n"
        assert config.count(anchor) == 2
        config = config.replace(anchor, LOCAL_DIRECTORY_BEFORE + anchor, 1)
        restart_node_bad_with(config, server_config=read_config("ldap_server.xml"))
        admin(node_bad, "SYSTEM RELOAD USERS")
        created_id = admin(
            node_bad, "SELECT id FROM system.roles WHERE name = 'role_b'"
        ).strip()
        assert role_grant_ids(node_bad, "johndoe", "role_b") == f"{created_id}\n"

        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                f"echo 'ATTACH ROLE role_b;' > /var/lib/clickhouse/access_after/{id_after}.sql",
            ],
            user="root",
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert role_grant_ids(node_bad, "johndoe", "role_b") == f"{created_id}\n"
        assert node_bad.contains_in_log(
            "Role 'role_b' exists in storages .local_directory., .local_directory_after.;"
            " the name resolves to the copy in .local_directory., which the users of LDAP directory .ldap. hold"
        )

        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                f"echo 'ATTACH ROLE role_b;' > /var/lib/clickhouse/access_before/{id_before}.sql",
            ],
            user="root",
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert role_grant_ids(node_bad, "johndoe", "role_b") == f"{id_before}\n"
        assert node_bad.contains_in_log(
            "Role 'role_b' exists in storages .local_directory_before., .local_directory., .local_directory_after.;"
            " the name resolves to the copy in .local_directory_before."
        )
    finally:
        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/access_after /var/lib/clickhouse/access_before",
            ],
            user="root",
        )
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


def test_interserver_materialised_user_promoted_by_a_run_needs_no_lookup(
    janedoe_in_role_a,
):
    """A fanout under the cluster secret materialises `promoted` on `node_bad` (`only_synced_users`
    false) without asking the directory, so `EXECUTE AS promoted` confirms the name upstream first.
    Once a run returns `promoted` in the snapshot, the name belongs to the synchronisation and
    `EXECUTE AS` resolves it from the snapshot alone: with the lookup identity broken afterwards it
    still works, as it does for every synchronised user."""
    base_entries = ldap_count_synced_entries()
    ldap_add_user("promoted")
    admin(node1, "CREATE USER promoted IDENTIFIED BY 'local'")
    try:
        admin(node1, "GRANT SELECT, SHOW COLUMNS, REMOTE ON *.* TO promoted")
        restart_node_bad_with(
            directories_bad_config(
                only_synced_users="false",
                create_roles="true",
                roles_storage="local_directory",
            ),
            server_config=read_config("ldap_server.xml"),
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, ldap_users_query("promoted")) == "0\n"

        # Whether the remote half may read is irrelevant; the name is materialised at authentication.
        node1.query_and_get_answer_with_error(
            "SELECT count() FROM clusterAllReplicas('test_ldap_cluster_bad', system.one)",
            user="promoted",
            password="local",
        )
        assert admin(node_bad, ldap_users_query("promoted")) == "1\n"

        ldap_set_memberships("promoted", {ROLE_A_GROUP})
        wait_ldap_synced_entries(base_entries + 1)
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert admin(node_bad, granted_roles_query("promoted")) == TSV([["role_a"]])

        # A lookup would fail from now on; a synchronised user does not need one.
        node_bad.replace_config(
            f"{CONFIG_D}/ldap_server_bad_lookup.xml",
            read_config("ldap_server_bad_lookup.xml"),
        )
        admin(node_bad, "SYSTEM RELOAD CONFIG")
        assert admin(node_bad, "EXECUTE AS promoted SELECT currentUser()") == TSV(
            [["promoted"]]
        )
    finally:
        admin(node1, "DROP USER IF EXISTS promoted")
        ldap_set_memberships("promoted", set())
        ldap_delete(user_dn("promoted"), ignore_missing=True)
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


def test_synced_direct_bind_login_needs_no_lookup_identity(janedoe_in_role_a):
    """In a synchronised directory a login verifies the password only. With a `bind_dn` template
    next to `lookup_bind_dn` (direct bind) the lookup identity serves the detection and the role
    searches, which such a login does not run, so a lookup password broken after a successful run
    must not lock the synchronised users out. `SYSTEM RELOAD CONFIG` also drops the verification
    cache, so the logins below reach the server."""
    try:
        server_config = read_config("ldap_server.xml").replace(
            "<bind_dn>{user_dn}</bind_dn>",
            f"<bind_dn>cn={{user_name}},{USERS_CONTAINER}</bind_dn>",
        )
        assert "{user_dn}</bind_dn>" not in server_config
        restart_node_bad_with(
            directories_bad_config(
                create_roles="true", roles_storage="local_directory"
            ),
            server_config=server_config,
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert login(node_bad, "janedoe") == TSV([["janedoe"]])

        broken_config = server_config.replace(
            f"<lookup_password>{LDAP_SERVICE_PASSWORD}</lookup_password>",
            "<lookup_password>wrong</lookup_password>",
        )
        assert broken_config != server_config
        node_bad.replace_config(f"{CONFIG_D}/ldap_server_bad_lookup.xml", broken_config)
        admin(node_bad, "SYSTEM RELOAD CONFIG")
        assert login(node_bad, "janedoe") == TSV([["janedoe"]])
        assert login(node_bad, "johndoe", "qwertz") == TSV([["johndoe"]])
        login_error(node_bad, "janedoe", "wrong")
    finally:
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


def test_renamed_role_follows_name_resolution(janedoe_in_role_a):
    """A granted role renamed into a name that resolves to a copy in an earlier storage must not be
    granted under the new name as well. `role_a` resolves to a hand-written copy in
    `local_directory_before`; the run-created `role_b`, granted to `johndoe`, is renamed into `role_a`
    on disk (as a restored file would be) and `johndoe` keeps exactly the copy the name resolves to.
    """
    id_before = "44444444-4444-4444-4444-444444444444"
    try:
        config = directories_bad_config(
            create_roles="true", roles_storage="local_directory"
        )
        anchor = "        <local_directory>\n"
        assert config.count(anchor) == 1
        config = config.replace(anchor, LOCAL_DIRECTORY_BEFORE + anchor)
        restart_node_bad_with(config, server_config=read_config("ldap_server.xml"))
        admin(node_bad, "SYSTEM RELOAD USERS")
        role_b_id = admin(
            node_bad, "SELECT id FROM system.roles WHERE name = 'role_b'"
        ).strip()

        admin(node_bad, "DROP ROLE role_a")
        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                f"echo 'ATTACH ROLE role_a;' > /var/lib/clickhouse/access_before/{id_before}.sql",
            ],
            user="root",
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert role_grant_ids(node_bad, "johndoe", "role_a") == f"{id_before}\n"

        node_bad.exec_in_container(
            [
                "bash",
                "-c",
                f"echo 'ATTACH ROLE role_a;' > /var/lib/clickhouse/access/{role_b_id}.sql",
            ],
            user="root",
        )
        admin(node_bad, "SYSTEM RELOAD USERS")
        assert role_grant_ids(node_bad, "johndoe", "role_a") == f"{id_before}\n"
        # The run created `role_b` again, since the renamed copy no longer carries the name.
        assert (
            admin(node_bad, "SELECT count() FROM system.roles WHERE name = 'role_b'")
            == "1\n"
        )
    finally:
        node_bad.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/access_before"], user="root"
        )
        restore_node_bad()
        admin(node_bad, "DROP ROLE IF EXISTS role_a, role_b")


def with_second_ldap_directory(name, sync=None, after=False):
    """`directories_bad.xml` with a second `ldap` directory named `name`, backed by the same server and
    declared right before (with `after`, right after) the synchronised one. `sync` is the content of its
    `<sync>` section; `None` gives a lazy directory, one without a section."""
    original = read_config("directories_bad.xml")
    sync_xml = f"            <sync>{sync}</sync>\n" if sync is not None else ""
    second = (
        "        <ldap>\n"
        f"            <name>{name}</name>\n"
        "            <server>openldap_strict</server>\n"
        f"{sync_xml}"
        "        </ldap>\n"
    )
    if after:
        anchor = "        </ldap>\n"
        assert original.count(anchor) == 1
        return original.replace(anchor, anchor + second)
    anchor = "        <ldap>\n            <server>openldap_strict</server>\n"
    assert original.count(anchor) == 1
    return original.replace(anchor, second + anchor)


def second_ldap_directory_refused(synced, other):
    """The startup error, as a `grep` pattern: the names are back-quoted in the message and a backtick
    inside the double-quoted `grep` argument would start a command substitution."""
    return (
        f"User directory .{synced}. has a 'sync' section and user directory .{other}. is another 'ldap'"
        " user directory: a synchronised 'ldap' user directory must be the only 'ldap' user directory"
        " in 'user_directories'"
    )


def test_startup_validation_of_a_second_ldap_directory():
    """The plan of a run asks the other storages whether they define a name and leaves it to a storage
    declared before the synchronised directory, which wins the login. A storage of any other type answers
    from its whole user set; another `ldap` directory never does: a lazy one knows only the users who have
    logged in through it, a synchronised one only its last snapshot, taken on its own schedule, so a user
    matching both would change directory with the timing of the runs. No run can make that sound, so the
    server refuses to start, whether the other `ldap` directory is lazy or synchronised too, declared
    before or after the synchronised one; every other kind of storage may still surround it (every node
    of this module has `users_xml` and a writable storage before its `ldap` directory).
    """
    assert_startup_fails_with(
        with_second_ldap_directory("ldap_lazy"),
        second_ldap_directory_refused("ldap", "ldap_lazy"),
    )
    assert_startup_fails_with(
        with_second_ldap_directory("ldap_lazy", after=True),
        second_ldap_directory_refused("ldap", "ldap_lazy"),
    )
    # Two synchronised directories, with the same search even: the first synchronised one is named first,
    # whether the second one is declared before or after it.
    assert_startup_fails_with(
        with_second_ldap_directory("ldap_first", sync_section()),
        second_ldap_directory_refused("ldap_first", "ldap"),
    )
    assert_startup_fails_with(
        with_second_ldap_directory("ldap_second", sync_section(), after=True),
        second_ldap_directory_refused("ldap", "ldap_second"),
    )


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
    # A duration that does not fit into the signed count would wrap into a negative wait.
    assert_startup_fails_with(
        directories_bad_config(interval="315360001"),
        "'interval' in 'user_directories.ldap.sync' section must not exceed 315360000 s (ten years), got 315360001",
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
