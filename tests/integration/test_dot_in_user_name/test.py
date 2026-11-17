import hashlib

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/default_profile.xml",
    ],
    user_configs=[
        "configs/users.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_user_with_dot_in_name():
    assert node.query("SELECT count()>0 FROM system.users where name = 'user.name'") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'user\\.name'") == "0\n"

    node.query("DROP USER IF EXISTS 'foo.bar'")
    node.query("CREATE USER 'foo.bar'")
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "1\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "0\n"

    node.query("ALTER USER 'foo.bar' RENAME TO 'foo\\.bar'")
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo.bar'") == "0\n"
    assert node.query("SELECT count()>0 FROM system.users where name = 'foo\\.bar'") == "1\n"
    node.query("DROP USER 'foo\\.bar'")


def test_role_with_dot_in_name():
    assert node.query(
        "SELECT count()>0 FROM system.roles WHERE name = 'my.role' AND storage = 'users_xml'"
    ) == "1\n"
    assert node.query(
        "SELECT count()>0 FROM system.roles WHERE name = 'my\\.role'"
    ) == "0\n"


def test_profile_with_dot_in_name():
    assert node.query(
        "SELECT count()>0 FROM system.settings_profiles WHERE name = 'my.profile' AND storage = 'users_xml'"
    ) == "1\n"
    assert node.query(
        "SELECT count()>0 FROM system.settings_profiles WHERE name = 'my\\.profile'"
    ) == "0\n"


def test_quota_with_dot_in_name():
    assert node.query(
        "SELECT count()>0 FROM system.quotas WHERE name = 'my.quota' AND storage = 'users_xml'"
    ) == "1\n"
    assert node.query(
        "SELECT count()>0 FROM system.quotas WHERE name = 'my\\.quota'"
    ) == "0\n"


def test_user_references_entities_with_dots():
    """Verify that a user whose name contains a dot is correctly associated with
    its settings profile, quota, and role when each of those entity names also contains a dot."""

    # The profile assignment flows through the user's ID and profile ID; both must be computed
    # from un-escaped names so the setting actually applies.
    assert node.query(
        "SELECT getSetting('max_memory_usage')",
        user="dotted.user",
    ) == "10000000\n"

    # The quota-to-user mapping is built from the user's ID; it must match the entity's un-escaped name.
    assert node.query(
        "SELECT count()>0 FROM system.quota_usage "
        "WHERE quota_name = 'my.quota' AND quota_key = 'dotted.user'",
        user="dotted.user",
    ) == "1\n"

    # The role ID must also be derived from the un-escaped name for the grant to take effect.
    assert node.query(
        "SELECT count()>0 FROM system.current_roles WHERE role_name = 'my.role'",
        user="dotted.user",
    ) == "1\n"


def test_row_policy_for_user_with_dot():
    """Verify that a row policy defined under `users.<name>.databases...filter` for a user
    whose name contains a dot is applied to that user. The policy's `to_roles` is derived from
    the user's ID, which must be computed from the un-escaped name for the filter to take effect."""

    node.query("DROP TABLE IF EXISTS default.test_row_policy")
    node.query("CREATE TABLE default.test_row_policy (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO default.test_row_policy VALUES (1), (2), (3)")

    # The row policy is stored under the un-escaped name.
    assert node.query(
        "SELECT count()>0 FROM system.row_policies "
        "WHERE short_name = 'dotted.user' AND database = 'default' AND table = 'test_row_policy'"
    ) == "1\n"
    assert node.query(
        "SELECT count()>0 FROM system.row_policies WHERE short_name = 'dotted\\.user'"
    ) == "0\n"

    # The filter `x = 1` must actually apply when the dotted user reads the table.
    assert node.query(
        "SELECT x FROM default.test_row_policy ORDER BY x",
        user="dotted.user",
    ) == "1\n"

    node.query("DROP TABLE default.test_row_policy")


def test_legacy_escaped_references_to_dotted_entities():
    """A config written for an older server may reference an entity defined in users.xml using the
    escaped spelling (e.g. <profile>my\\.profile</profile>) that used to be required to match the
    previously-stored escaped name. Such references must still resolve after the un-escaping change."""

    # Profile reference written as `my\.profile` must still apply the profile's settings.
    assert node.query(
        "SELECT getSetting('max_memory_usage')",
        user="legacy.user",
    ) == "10000000\n"

    # Quota reference written as `my\.quota` must still associate the user with the quota.
    assert node.query(
        "SELECT count()>0 FROM system.quota_usage "
        "WHERE quota_name = 'my.quota' AND quota_key = 'legacy.user'",
        user="legacy.user",
    ) == "1\n"

    # Role granted as `my\.role` must still take effect.
    assert node.query(
        "SELECT count()>0 FROM system.current_roles WHERE role_name = 'my.role'",
        user="legacy.user",
    ) == "1\n"


def _users_xml_id(name, unique_char):
    """Mirror of UsersConfigParser::generateID for users.xml entities."""
    digest = hashlib.md5(
        name.encode() + (unique_char + "USRSXML").encode()
    ).digest()
    b = bytes(
        [
            digest[7], digest[6], digest[5], digest[4],
            digest[3], digest[2],
            digest[1], digest[0],
            digest[15], digest[14],
            digest[13], digest[12], digest[11], digest[10], digest[9], digest[8],
        ]
    ).hex()
    return f"{b[0:8]}-{b[8:12]}-{b[12:16]}-{b[16:20]}-{b[20:32]}"


def test_ids_of_dotted_entities_are_unchanged():
    """Roles, quotas, and settings profiles from users.xml used to be stored under the escaped names (`my\\.role`),
    and their IDs were generated from these names. References persisted in other access storages hold these IDs,
    so un-escaping the names must not change the IDs."""

    for table, name, unique_char in [
        ("system.roles", "my.role", "R"),
        ("system.quotas", "my.quota", "Q"),
        ("system.settings_profiles", "my.profile", "S"),
    ]:
        assert (
            node.query(f"SELECT id FROM {table} WHERE name = '{name}'").strip()
            == _users_xml_id(name.replace(".", "\\."), unique_char)
        )


def test_escaped_default_profile_in_server_config():
    """The server config references `server.profile` as `<default_profile>server\\.profile</default_profile>`.
    The server must start, and the settings of the profile must apply."""
    assert node.query("SELECT getSetting('max_query_size')") == "262145\n"


def test_row_policy_for_dotted_database_and_table():
    """The database and table names of a row policy also come from config keys with escaped dots."""
    node.query("DROP TABLE IF EXISTS default.`t.dot`")
    node.query("DROP DATABASE IF EXISTS `db.dot`")
    node.query("CREATE TABLE default.`t.dot` (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO default.`t.dot` VALUES (1), (2), (3)")
    node.query("CREATE DATABASE `db.dot`")
    node.query("CREATE TABLE `db.dot`.t (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO `db.dot`.t VALUES (1), (2), (3)")

    assert node.query("SELECT x FROM default.`t.dot` ORDER BY x", user="dotted.user") == "2\n"
    assert node.query("SELECT x FROM `db.dot`.t ORDER BY x", user="dotted.user") == "3\n"

    node.query("DROP TABLE default.`t.dot`")
    node.query("DROP DATABASE `db.dot`")


def test_old_spelling_of_names_is_resolved():
    """Names of roles, quotas, and settings profiles can be passed by name in the old escaped spelling,
    e.g. by a server of an older version in a distributed query, or in a queued `ON CLUSTER` task."""
    assert node.query(
        "SET ROLE `my\\.role`; SELECT role_name FROM system.current_roles",
        user="dotted.user",
    ) == "my.role\n"

    assert node.query(
        "SET profile = 'my\\.profile'; SELECT getSetting('max_memory_usage')"
    ) == "10000000\n"
