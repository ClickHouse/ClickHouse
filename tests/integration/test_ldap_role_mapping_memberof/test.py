"""
`role_mapping` with `rdn_attribute` and a `groups` allow-list.

Active Directory reports group membership as full group DNs (`memberOf`). The shared
OpenLDAP fixture has no `memberOf` overlay, so the same shape is produced by the reverse
group search with `attribute` = `dn`: every value is a group DN such as
`cn=clickhouse-role_1,ou=groups,dc=example,dc=org`, `rdn_attribute` = `cn` extracts the
CN, and `groups` entries in both DN form and plain form are exercised against it. A true
`memberOf` self-lookup (`base_dn` = `{user_dn}`, `scope` = `base`) should be added once a
fixture with the `memberof` overlay is available.

Group DNs of interest returned by the search on `instance` (see `configs/ldap_with_groups.xml`):
  - `cn=clickhouse-role_1,ou=groups,...`  matches the DN-form entry          -> `role_1`
  - `cn=clickhouse-ROLE_2,...`            matches the plain entry case-insensitively, the
                                          configured spelling `clickhouse-role_2` wins -> `role_2`
  - `cn=clickhouse-role_3,ou=groups,...`  not allow-listed                   -> nothing
  - `cn=clickhouse-role_1,ou=other,...`   same CN, other container: the DN form pins the
                                          container and the plain list has no such entry -> nothing
  - `cn=clickhouse-grp\\, x,...`           unescaped RDN value matches the plain entry -> `grp, x`
  - `cn=legacy-role_4,...`                mapped by the legacy `role_mapping` only    -> `role_4`

`instance_invalid` restarts with invalid configurations to check the startup validation:
every `groups` entry that could never grant a role (empty, unparsable DN, DN without the
`rdn_attribute` RDN, duplicate, or not carrying the configured `prefix`) is rejected at startup.
"""

import json
import logging
import os
import shlex

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

LDAP_ADMIN_BIND_DN = "cn=admin,dc=example,dc=org"
LDAP_ADMIN_PASSWORD = "clickhouse"

USERS_CONTAINER = "ou=users,dc=example,dc=org"
GROUPS_CONTAINER = "ou=groups,dc=example,dc=org"
OTHER_CONTAINER = "ou=other,dc=example,dc=org"

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
LDAP_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/ldap_with_groups.xml"
ERR_LOG = "clickhouse-server.err.log"

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/ldap_with_groups.xml"],
    user_configs=["configs/users.xml"],
    with_ldap=True,
)

instance_invalid = cluster.add_instance(
    "instance_invalid",
    main_configs=["configs/ldap_with_groups.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


def ldap_run(command):
    code, (stdout, stderr) = cluster.ldap_container.exec_run(
        ["sh", "-c", command], demux=True
    )
    logging.debug(
        f"test_ldap_role_mapping_memberof code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert code == 0, f"LDAP command failed with code {code}: {stderr}"


def ldap_add(ldif):
    # `printf '%s'` keeps backslashes in the LDIF (e.g. `cn=grp\, x`) intact, unlike `echo`.
    ldap_run(
        "printf '%s\\n' {ldif} | ldapadd -H ldap://{host}:{port} -D {admin_bind_dn} -x -w {admin_password}".format(
            ldif=shlex.quote(ldif),
            host=cluster.ldap_host,
            port=cluster.ldap_port,
            admin_bind_dn=shlex.quote(LDAP_ADMIN_BIND_DN),
            admin_password=shlex.quote(LDAP_ADMIN_PASSWORD),
        )
    )


def ldap_delete(dn, ignore_missing=False):
    command = "ldapdelete -H ldap://{host}:{port} -D {admin_bind_dn} -x -w {admin_password} {dn}".format(
        host=cluster.ldap_host,
        port=cluster.ldap_port,
        admin_bind_dn=shlex.quote(LDAP_ADMIN_BIND_DN),
        admin_password=shlex.quote(LDAP_ADMIN_PASSWORD),
        dn=shlex.quote(dn),
    )
    if ignore_missing:
        # `ldapdelete` exits with 32 (`No such object`) when the entry is already gone;
        # any other failure still fails the test.
        command += " || [ $? -eq 32 ]"
    ldap_run(command)


def add_organizational_unit(ou_dn, ou_name):
    ldap_add(
        f"dn: {ou_dn}\n"
        "objectClass: top\n"
        "objectClass: organizationalUnit\n"
        f"ou: {ou_name}\n"
    )


def add_group(group_rdn_value, container, member_cn, group_dn=None):
    # `group_dn` is only needed when the RDN value contains characters that must be escaped in a DN.
    if group_dn is None:
        group_dn = f"cn={group_rdn_value},{container}"
    ldap_add(
        f"dn: {group_dn}\n"
        "objectClass: top\n"
        "objectClass: groupOfNames\n"
        f"cn: {group_rdn_value}\n"
        f"member: cn={member_cn},{USERS_CONTAINER}\n"
    )


def current_roles(node, user, password):
    return node.query(
        "SELECT role_name FROM system.current_roles ORDER BY role_name",
        user=user,
        password=password,
    )


def admin_query(node, query):
    return node.query(query, user="common_user", password="qwerty")


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    try:
        cluster.start()
        add_organizational_unit(GROUPS_CONTAINER, "groups")
        add_organizational_unit(OTHER_CONTAINER, "other")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
def roles():
    role_names = ["role_1", "role_2", "role_3", "role_4", "`grp, x`"]
    for role_name in role_names:
        admin_query(instance, f"DROP ROLE IF EXISTS {role_name}")
        admin_query(instance, f"CREATE ROLE {role_name}")
    try:
        yield
    finally:
        for role_name in role_names:
            admin_query(instance, f"DROP ROLE IF EXISTS {role_name}")


def test_authentication_without_groups_succeeds():
    # Neither user belongs to any allow-listed group yet: the mappings grant nothing, but a
    # login still works (the fixture's default `cn=readers` group is ignored by every mapping).
    assert instance.query(
        "SELECT currentUser()", user="janedoe", password="qwerty"
    ) == TSV([["janedoe"]])
    assert current_roles(instance, "janedoe", "qwerty") == ""


def test_groups_allow_list(roles):
    add_group("clickhouse-role_1", GROUPS_CONTAINER, "johndoe")
    # Stored with a different letter case than the configured plain entry `clickhouse-role_2`.
    add_group("clickhouse-ROLE_2", GROUPS_CONTAINER, "johndoe")
    # Exists as a role, but is not allow-listed.
    add_group("clickhouse-role_3", GROUPS_CONTAINER, "johndoe")
    try:
        # `role_1` via the DN-form entry (configured with different case and extra spaces),
        # `role_2` via the case-insensitive plain entry with the configured spelling, nothing
        # for `role_3`, and nothing from the `rdn_attribute` = `ou` mapping.
        assert current_roles(instance, "johndoe", "qwertz") == TSV(
            [["role_1"], ["role_2"]]
        )

        # Membership changes are picked up on the next login.
        ldap_delete(f"cn=clickhouse-role_1,{GROUPS_CONTAINER}")
        assert current_roles(instance, "johndoe", "qwertz") == TSV([["role_2"]])
    finally:
        # `clickhouse-role_1` is already gone when the test passed.
        for group_cn in ["clickhouse-role_1", "clickhouse-ROLE_2", "clickhouse-role_3"]:
            ldap_delete(f"cn={group_cn},{GROUPS_CONTAINER}", ignore_missing=True)


def test_dn_form_group_pins_the_container(roles):
    # Same CN as the DN-form entry, but in another container: it must not grant `role_1`.
    add_group("clickhouse-role_1", OTHER_CONTAINER, "janedoe")
    # The plain-form entry matches the CN in any container.
    add_group("clickhouse-role_2", OTHER_CONTAINER, "janedoe")
    try:
        assert current_roles(instance, "janedoe", "qwerty") == TSV([["role_2"]])
    finally:
        ldap_delete(f"cn=clickhouse-role_1,{OTHER_CONTAINER}")
        ldap_delete(f"cn=clickhouse-role_2,{OTHER_CONTAINER}")


def test_escaped_rdn_value(roles):
    # The comma is escaped in the DN and unescaped by `rdn_attribute` extraction before the
    # comparison with the plain entry `clickhouse-grp, x`.
    group_dn = f"cn=clickhouse-grp\\, x,{GROUPS_CONTAINER}"
    add_group("clickhouse-grp, x", GROUPS_CONTAINER, "johndoe", group_dn=group_dn)
    try:
        assert current_roles(instance, "johndoe", "qwertz") == TSV([["grp, x"]])
    finally:
        ldap_delete(group_dn)


def test_legacy_role_mapping_still_works(roles):
    # Mapped by the second `role_mapping` (attribute `cn`, prefix `legacy-`, no new keys);
    # the first mapping ignores the DN because it is not allow-listed.
    add_group("legacy-role_4", GROUPS_CONTAINER, "johndoe")
    try:
        assert current_roles(instance, "johndoe", "qwertz") == TSV([["role_4"]])
    finally:
        ldap_delete(f"cn=legacy-role_4,{GROUPS_CONTAINER}")


def test_user_directories_params():
    params = json.loads(
        admin_query(
            instance,
            "SELECT params FROM system.user_directories WHERE type = 'ldap'",
        ).strip()
    )
    role_mappings = params["role_mappings"]
    assert len(role_mappings) == 3

    assert role_mappings[0]["rdn_attribute"] == "cn"
    assert role_mappings[0]["groups"] == [
        "CN=clickhouse-role_1, OU=groups, DC=example, DC=org",
        "clickhouse-role_2",
        "clickhouse-grp, x",
    ]

    # Mappings without the new keys expose them with empty values.
    assert role_mappings[1]["prefix"] == "legacy-"
    assert role_mappings[1]["rdn_attribute"] == ""
    assert role_mappings[1]["groups"] == []

    assert role_mappings[2]["rdn_attribute"] == "ou"
    assert role_mappings[2]["groups"] == []


def assert_startup_fails_with(bad_config, expected_message):
    instance_invalid.stop_clickhouse()
    try:
        instance_invalid.copy_file_to_container(
            os.path.join(CONFIG_DIR, bad_config), LDAP_CONFIG_IN_CONTAINER
        )
        instance_invalid.start_clickhouse(expected_to_fail=True)
        assert (
            instance_invalid.grep_in_log(
                expected_message, filename=ERR_LOG, only_latest=True
            )
            != ""
        )
    finally:
        instance_invalid.copy_file_to_container(
            os.path.join(CONFIG_DIR, "ldap_with_groups.xml"), LDAP_CONFIG_IN_CONTAINER
        )
        instance_invalid.start_clickhouse()


def test_startup_fails_on_empty_group():
    assert_startup_fails_with("ldap_empty_group.xml", "Empty 'group' entry")


def test_startup_fails_on_rdn_attribute_without_groups_or_prefix():
    assert_startup_fails_with(
        "ldap_rdn_attribute_without_groups_or_prefix.xml",
        "'rdn_attribute' in 'user_directories.ldap.role_mapping' section requires a non-empty 'groups' list or a non-empty 'prefix'",
    )


def test_startup_fails_on_empty_rdn_attribute():
    assert_startup_fails_with(
        "ldap_empty_rdn_attribute.xml", "Empty 'rdn_attribute' entry"
    )


def test_startup_fails_on_dn_group_without_rdn_attribute():
    assert_startup_fails_with(
        "ldap_dn_group_without_rdn_attribute.xml",
        "treated as a DN, which requires 'rdn_attribute' to be set",
    )


def test_startup_fails_on_invalid_dn_group():
    # `cn=clickhouse-role_1,,dc=example,dc=org` contains `=` and is therefore parsed as a DN, which fails.
    assert_startup_fails_with("ldap_invalid_dn_group.xml", "is not a valid DN")


def test_startup_fails_on_dn_group_without_rdn():
    # `ou=groups,dc=example,dc=org` is a valid DN, but nothing in it could become the role name.
    assert_startup_fails_with("ldap_dn_group_without_rdn.xml", "has no 'cn' RDN")


def test_startup_fails_on_duplicate_group():
    # The two entries differ in letter case and whitespace only and normalize to the same DN.
    assert_startup_fails_with("ldap_duplicate_group.xml", "Duplicate group")


def test_startup_fails_on_group_without_prefix():
    # `prefix` is compared case-sensitively with the configured spelling, so `CLICKHOUSE-ROLE_2`
    # with `prefix` `clickhouse-` could never grant a role and is rejected instead of silently ignored.
    assert_startup_fails_with(
        "ldap_group_without_prefix.xml",
        "does not start with the configured prefix 'clickhouse-'",
    )
