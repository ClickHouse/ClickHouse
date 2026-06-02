import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
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
