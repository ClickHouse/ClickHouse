import hashlib

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
    stay_alive=True,
)
# A node starting on an older version that stores a dotted entity name in its escaped spelling, used to
# verify that references persisted by that version still resolve after upgrading to the un-escaping version.
node_bc = cluster.add_instance(
    "node_bc",
    user_configs=[
        "configs/users_bc.xml",
    ],
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
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


def test_disk_grant_with_legacy_escaped_role_id_resolves():
    """A grant persisted in a non-XML storage references a users.xml role by its raw UUID. An older server
    generated that UUID from the escaped name (`my\\.role`); after un-escaping the role is loaded under a
    different UUID. The legacy UUID must still resolve to the role, otherwise the grant is silently dropped."""

    canonical = _users_xml_id("my.role", "R")
    legacy = _users_xml_id("my\\.role", "R")
    assert canonical != legacy

    node.query("DROP USER IF EXISTS disk_user")
    node.query("CREATE USER disk_user")
    node.query("GRANT `my.role` TO disk_user")
    # Self-check the id encoding against the server: the live role must carry the canonical UUID.
    assert (
        node.query("SELECT id FROM system.roles WHERE name = 'my.role'").strip()
        == canonical
    )

    # Rewrite the persisted grant to the legacy UUID, reproducing what an older server stored on disk.
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"sed -i 's/{canonical}/{legacy}/g' /var/lib/clickhouse/access/*.sql"]
    )
    node.start_clickhouse()

    assert "my.role" in node.query("SHOW GRANTS FOR disk_user")
    node.query("DROP USER disk_user")


def test_backward_compat_legacy_escaped_user_id_resolves():
    """End-to-end backward compatibility: an older server stores the dotted user under its escaped name
    `bc\\.user` and persists a quota in disk storage referencing that user by the escaped-name UUID. After
    upgrading to the un-escaping version the user is loaded under `bc.user` with a different UUID, so the
    quota's reference must be bridged for it to still apply."""

    # On the old server the dotted user is stored escaped, so reference it with the backslash doubled.
    node_bc.query(
        "CREATE QUOTA test_bc_quota FOR INTERVAL 1 hour MAX queries = 100 TO `bc\\\\.user`"
    )
    assert "bc.user" in node_bc.query("SHOW CREATE QUOTA test_bc_quota").replace(
        "\\.", "."
    )

    node_bc.restart_with_latest_version()

    # The user is now loaded un-escaped; the quota persisted by the old server must still resolve to it.
    assert node_bc.query("SELECT count() FROM system.users WHERE name = 'bc.user'") == "1\n"
    assert "bc.user" in node_bc.query("SHOW CREATE QUOTA test_bc_quota")
    node_bc.query("DROP QUOTA test_bc_quota")
