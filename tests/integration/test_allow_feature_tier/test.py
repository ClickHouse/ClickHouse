from contextlib import contextmanager
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/access_control_path.xml",
        "configs/allow_feature_tier.xml",
        "configs/backups_disk.xml",
        "configs/custom_settings_prefix.xml",
        "configs/memory_access_storage.xml",
    ],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    stay_alive=True,
)

# Boots with a non-zero feature tier, a `merge_tree_`-prefixed constraint and `compatibility` in the
# default profile, and an EXPERIMENTAL `MergeTree` setting in the server config.
instance_with_merge_tree_constraint = cluster.add_instance(
    "instance_with_merge_tree_constraint",
    main_configs=[
        "configs/allow_feature_tier_1.xml",
        "configs/merge_tree_experimental_setting.xml",
    ],
    user_configs=[
        "configs/users.d/merge_tree_constraint.xml",
    ],
    stay_alive=True,
)

# Two servers sharing a replicated access storage, which are also used as replicas of a `Replicated`
# database. They disagree on which settings are allowed: the first allows every tier, while the second
# refuses EXPERIMENTAL settings.
permissive_replica = cluster.add_instance(
    "permissive_replica",
    main_configs=[
        "configs/allow_feature_tier.xml",
        "configs/replicated_access_storage.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
)
strict_replica = cluster.add_instance(
    "strict_replica",
    main_configs=[
        "configs/allow_feature_tier_1.xml",
        "configs/replicated_access_storage.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
)

instance_with_legacy_constraints = cluster.add_instance(
    "instance_with_legacy_constraints",
    main_configs=[
        "configs/allow_feature_tier.xml",
        "configs/settings_constraints_keep_previous.xml",
    ],
    stay_alive=True,
)

feature_tier_path = "/etc/clickhouse-server/config.d/allow_feature_tier.xml"
feature_tier_1_path = "/etc/clickhouse-server/config.d/allow_feature_tier_1.xml"

# These settings are used as examples of their tier. If one changes tier in the future, please replace
# it with another setting of the same tier. If there is none, feel free to comment out the affected test.
EXPERIMENTAL_SETTING = (
    "allow_experimental_funnel_functions"  # also in configs/users.d/users.xml
)
BETA_SETTING = "allow_experimental_lightweight_update"
PRIVATE_PREVIEW_SETTING = "distributed_plan_workers_num"

# A `MergeTree` setting is written by its bare name in a table's own `SETTINGS` or `ALTER ... MODIFY
# SETTING`, and with a `merge_tree_` prefix in a profile, user or session `SETTINGS` clause.
MERGE_TREE_SETTINGS_PREFIX = "merge_tree_"
MERGE_TREE_PRODUCTION_SETTING = "max_avg_part_size_for_too_many_parts"
MERGE_TREE_EXPERIMENTAL_SETTING = "allow_experimental_replacing_merge_with_cleanup"
# Set by configs/merge_tree_experimental_setting.xml
MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG = "allow_commit_order_projection"
MERGE_TREE_ALIASED_SETTING = "allow_experimental_block_number_column"
MERGE_TREE_ALIASED_SETTING_CANONICAL = "enable_block_number_column"

MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE = (
    MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_PRODUCTION_SETTING
)
MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE = (
    MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_EXPERIMENTAL_SETTING
)

# Must match configs/users.d/merge_tree_constraint.xml
MERGE_TREE_PRODUCTION_MIN = 536870912
MERGE_TREE_PRODUCTION_MAX = 2147483648
MERGE_TREE_PRODUCTION_VALUE = 1073741824

# Allowed by configs/custom_settings_prefix.xml
CUSTOM_SETTING = "custom_setting_of_this_test"

# Must match configs/users.d/merge_tree_constraint.xml. The default of this setting is below the minimum
# the profile declares, so resetting it to the default is refused.
MERGE_TREE_SETTING_WITH_A_FORBIDDEN_DEFAULT = (
    MERGE_TREE_SETTINGS_PREFIX + "index_granularity"
)
MERGE_TREE_FORBIDDEN_DEFAULT_MIN = 16384

EXPERIMENTAL_BLOCKED = "Changes to EXPERIMENTAL settings are disabled"
BETA_BLOCKED = "Changes to BETA settings are disabled"
PRIVATE_PREVIEW_BLOCKED = "Changes to PRIVATE PREVIEW settings are disabled"
ACCESS_CONTROL_FEATURE_TIER_FAILPOINT = (
    "access_control_pause_after_feature_tier_check"
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_current_tier_value(node):
    query_with_current_tier_value = (
        "SELECT value FROM system.server_settings where name = 'allow_feature_tier'"
    )
    return node.query(query_with_current_tier_value).strip()


def set_feature_tier(node, old_value, new_value, config_path=feature_tier_path):
    node.replace_in_config(config_path, old_value, new_value)
    node.query("SYSTEM RELOAD CONFIG")
    assert new_value == get_current_tier_value(node)


def read_experimental_setting(node, user=None):
    query = f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'"
    if user is None:
        return node.query(query).strip()
    return node.query(query, user=user).strip()


def drop_entities(node, users=(), roles=(), profiles=(), storage=None):
    storage_clause = f" FROM {storage}" if storage else ""
    for user in users:
        node.query(f"DROP USER IF EXISTS {user}{storage_clause}")
    for role in roles:
        node.query(f"DROP ROLE IF EXISTS {role}{storage_clause}")
    for profile in profiles:
        node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}{storage_clause}")


@contextmanager
def feature_tier(node, value, config_path=feature_tier_path):
    old_value = get_current_tier_value(node)
    set_feature_tier(node, old_value, value, config_path)
    try:
        yield
    finally:
        set_feature_tier(node, value, old_value, config_path)


def assert_experimental_change_is_blocked(node, statement, **kwargs):
    output, error = node.query_and_get_answer_with_error(statement, **kwargs)
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error, statement + ": " + error


def test_allow_feature_tier_in_general_settings(start_cluster):
    query_with_experimental_setting = f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=1"
    query_with_beta_setting = f"SELECT 1 SETTINGS {BETA_SETTING}=1"

    assert "0" == get_current_tier_value(instance)
    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_setting
    )
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental settings
    set_feature_tier(instance, "0", "1")
    assert_experimental_change_is_blocked(instance, query_with_experimental_setting)

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental and private preview settings. Beta settings are still allowed.
    set_feature_tier(instance, "1", "2")
    assert_experimental_change_is_blocked(instance, query_with_experimental_setting)

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental, private preview and beta settings
    set_feature_tier(instance, "2", "3")
    assert_experimental_change_is_blocked(instance, query_with_experimental_setting)

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert output == ""
    assert BETA_BLOCKED in error

    # Leave the server as it was
    set_feature_tier(instance, "3", "0")


def test_allow_feature_tier_in_private_preview_settings(start_cluster):
    query_with_private_preview_setting = (
        f"SELECT 1 SETTINGS {PRIVATE_PREVIEW_SETTING}=1"
    )
    query_with_experimental_setting = f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=1"

    assert "0" == get_current_tier_value(instance)
    output, error = instance.query_and_get_answer_with_error(
        query_with_private_preview_setting
    )
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental settings; private preview is still allowed
    set_feature_tier(instance, "0", "1")

    output, error = instance.query_and_get_answer_with_error(
        query_with_private_preview_setting
    )
    assert error == ""
    assert "1" == output.strip()

    assert_experimental_change_is_blocked(instance, query_with_experimental_setting)

    # Disable private preview settings too
    set_feature_tier(instance, "1", "2")

    output, error = instance.query_and_get_answer_with_error(
        query_with_private_preview_setting
    )
    assert output == ""
    assert PRIVATE_PREVIEW_BLOCKED in error

    # Disable beta settings as well; private preview stays blocked
    set_feature_tier(instance, "2", "3")

    output, error = instance.query_and_get_answer_with_error(
        query_with_private_preview_setting
    )
    assert output == ""
    assert PRIVATE_PREVIEW_BLOCKED in error

    # Leave the server as it was
    set_feature_tier(instance, "3", "0")


def test_allow_feature_tier_in_mergetree_settings(start_cluster):
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")

    # Disable experimental settings
    set_feature_tier(instance, "0", "1")

    query_with_experimental_mergetree_setting = f"""
        CREATE TABLE test_experimental (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING}=1;
    """

    assert_experimental_change_is_blocked(
        instance, query_with_experimental_mergetree_setting
    )

    # Go back
    set_feature_tier(instance, "1", "0")

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_mergetree_setting
    )
    assert output == ""
    assert error == ""

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_experimental'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING in output

    # We now disable experimental settings and restart the server to confirm it boots correctly
    set_feature_tier(instance, "0", "1")

    instance.restart_clickhouse()

    # After the reboot the table will be there
    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_experimental'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING in output

    # Creating a different table should not be possible
    assert_experimental_change_is_blocked(
        instance,
        f"""
            CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
            ENGINE = ReplacingMergeTree(version, is_deleted)
            ORDER by (uid)
            SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING}=1;
        """,
    )

    # Creating a different table and altering its settings to enable experimental should not be possible either
    output, error = instance.query_and_get_answer_with_error("""
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid);
    """)
    assert output == ""
    assert error == ""

    assert_experimental_change_is_blocked(
        instance,
        f"ALTER TABLE test_experimental_new MODIFY setting {MERGE_TREE_EXPERIMENTAL_SETTING}=1",
    )
    instance.query("DROP TABLE IF EXISTS test_experimental_new")

    set_feature_tier(instance, "1", "0")
    instance.query("DROP TABLE IF EXISTS test_experimental")


def test_allow_feature_tier_in_mergetree_settings_with_old_compatibility(start_cluster):
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")

    # Disable experimental settings
    set_feature_tier(instance, "0", "1")

    basic_merge_tree_query = """
        create table b (a Int64) ENGINE=MergeTree() order by a;
    """

    output, error = instance.query_and_get_answer_with_error(basic_merge_tree_query)
    assert output == ""
    assert error == ""

    # Go back
    set_feature_tier(instance, "1", "0")
    instance.query("DROP TABLE IF EXISTS b")


def test_allow_feature_tier_in_user(start_cluster):
    drop_entities(instance, users=["user_experimental"])
    assert "0" == get_current_tier_value(instance)

    # Disable experimental settings
    set_feature_tier(instance, "0", "1")
    assert_experimental_change_is_blocked(
        instance,
        f"CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1",
    )

    # Go back to normal and create the user to restart the server and verify it works
    set_feature_tier(instance, "1", "0")

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert output == ""
    assert error == ""

    # Default user = 0
    assert read_experimental_setting(instance) == "0"

    # New user = 1
    assert read_experimental_setting(instance, "user_experimental") == "1"

    # Change back to block experimental features and restart to confirm everything is working as expected (only new changes are blocked)
    set_feature_tier(instance, "0", "1")

    instance.restart_clickhouse()

    # Default user = 0
    assert read_experimental_setting(instance) == "0"

    # New user = 1
    assert read_experimental_setting(instance, "user_experimental") == "1"

    # But note that they can't change the value either
    # 1 - 1 => OK
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=1",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""
    # 1 - 0 => KO
    assert_experimental_change_is_blocked(
        instance,
        f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=0",
        user="user_experimental",
    )

    set_feature_tier(instance, "1", "0")
    drop_entities(instance, users=["user_experimental"])


def test_it_is_possible_to_enable_experimental_settings_in_default_profile(
    start_cluster,
):
    # You can disable changing experimental settings but changing the default value via global config file is ok
    # It will just make the default value different and block changes
    instance.replace_in_config(feature_tier_path, "0", "2")

    # Change default user config
    instance.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml",
        f"{EXPERIMENTAL_SETTING}>.",
        f"{EXPERIMENTAL_SETTING}>1",
    )

    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)
    assert read_experimental_setting(instance) == "1"

    # But it won't be possible to change it
    assert_experimental_change_is_blocked(
        instance, f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=0"
    )

    instance.replace_in_config(feature_tier_path, "2", "0")
    instance.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml",
        f"{EXPERIMENTAL_SETTING}>.",
        f"{EXPERIMENTAL_SETTING}>0",
    )

    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)


def get_profile_element(node, profile_name, setting_name):
    query = (
        "SELECT min, max FROM system.settings_profile_elements "
        f"WHERE profile_name = '{profile_name}' AND setting_name = '{setting_name}'"
    )
    return node.query(query).strip()


def test_allow_feature_tier_with_merge_tree_prefixed_profile_elements(start_cluster):
    # We use these settings as an example. If it fails in the future because the tier of the setting changed,
    # please replace it with another setting in the same tier
    assert "0" == get_current_tier_value(instance)

    def drop_objects():
        drop_entities(
            instance,
            users=["user_with_merge_tree_element"],
            profiles=["profile_with_merge_tree_element"],
        )

    drop_objects()

    for tier in ["1", "2"]:
        set_feature_tier(instance, "0", tier)

        # A constraint on a PRODUCTION `MergeTree` setting is allowed at any tier
        output, error = instance.query_and_get_answer_with_error(
            "CREATE SETTINGS PROFILE profile_with_merge_tree_element SETTINGS "
            f"{MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} MIN {MERGE_TREE_PRODUCTION_MIN} MAX {MERGE_TREE_PRODUCTION_MAX}"
        )
        assert output == ""
        assert error == ""

        element = get_profile_element(
            instance,
            "profile_with_merge_tree_element",
            MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE,
        )
        assert str(MERGE_TREE_PRODUCTION_MIN) in element
        assert str(MERGE_TREE_PRODUCTION_MAX) in element

        # A value is allowed too, both for a user and in a query
        output, error = instance.query_and_get_answer_with_error(
            "CREATE USER user_with_merge_tree_element IDENTIFIED WITH no_password "
            f"SETTINGS {MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} = {MERGE_TREE_PRODUCTION_VALUE}"
        )
        assert output == ""
        assert error == ""

        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} = {MERGE_TREE_PRODUCTION_VALUE}"
        )
        assert output.strip() == "1"
        assert error == ""

        # EXPERIMENTAL is rejected because of its tier, not because the name is unknown
        assert_experimental_change_is_blocked(
            instance,
            "CREATE SETTINGS PROFILE profile_with_experimental_merge_tree_element SETTINGS "
            f"{MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE} = 1",
        )

        # The server boots with those objects in place
        instance.restart_clickhouse()
        assert tier == get_current_tier_value(instance)
        assert (
            "1"
            == instance.query(
                "SELECT count() FROM system.settings_profiles WHERE name = 'profile_with_merge_tree_element'"
            ).strip()
        )

        drop_objects()
        set_feature_tier(instance, tier, "0")


def test_merge_tree_constraint_in_config_with_feature_tier(start_cluster):
    # The server must start with such a constraint in the config and the feature tier not 0
    node = instance_with_merge_tree_constraint
    assert "1" == get_current_tier_value(node)
    assert "1" == node.query("SELECT 1").strip()

    element = get_profile_element(
        node, "default", MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE
    )
    assert str(MERGE_TREE_PRODUCTION_MIN) in element
    assert str(MERGE_TREE_PRODUCTION_MAX) in element

    # And the constraint is enforced
    node.query("DROP TABLE IF EXISTS test_merge_tree_constraint")
    output, error = node.query_and_get_answer_with_error(
        "CREATE TABLE test_merge_tree_constraint (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_PRODUCTION_SETTING} = 1"
    )
    assert output == ""
    assert f"shouldn't be less than {MERGE_TREE_PRODUCTION_MIN}" in error
    node.query("DROP TABLE IF EXISTS test_merge_tree_constraint")

    node.restart_clickhouse()
    assert "1" == node.query("SELECT 1").strip()


def test_server_level_merge_tree_settings_are_not_blocked_by_feature_tier(
    start_cluster,
):
    # `compatibility` and the `merge_tree` config section can change the value of EXPERIMENTAL/BETA settings.
    # They are set by the server, not by a query, so they must not make every table creation fail
    node = instance_with_merge_tree_constraint
    assert (
        "24.10"
        == node.query(
            "SELECT value FROM system.settings WHERE name = 'compatibility'"
        ).strip()
    )
    assert (
        "1"
        == node.query(
            f"SELECT value FROM system.merge_tree_settings WHERE name = '{MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG}'"
        ).strip()
    )

    for tier in ["1", "2"]:
        if tier != "1":
            set_feature_tier(node, "1", tier, feature_tier_1_path)
        assert tier == get_current_tier_value(node)

        node.query("DROP TABLE IF EXISTS test_server_level_settings")
        output, error = node.query_and_get_answer_with_error(
            "CREATE TABLE test_server_level_settings (a UInt64) ENGINE = MergeTree ORDER BY a"
        )
        assert output == ""
        assert error == ""

        # Session settings keep working with `compatibility`, including `merge_tree_`-prefixed ones
        output, error = node.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS compatibility = '24.10', {MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} = {MERGE_TREE_PRODUCTION_VALUE}"
        )
        assert output.strip() == "1"
        assert error == ""

        # Re-declaring the value already in effect (forced by the server) from a query is a no-op, exactly
        # like it is for a plain session/query setting, so it is allowed
        output, error = node.query_and_get_answer_with_error(
            f"ALTER TABLE test_server_level_settings MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 1"
        )
        assert output == ""
        assert error == ""

        # But reverting it to the compiled default is a real change and is rejected, even though the
        # resulting value matches the compiled default
        assert_experimental_change_is_blocked(
            node,
            f"ALTER TABLE test_server_level_settings MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0",
        )

        # Same for `CREATE TABLE`
        assert_experimental_change_is_blocked(
            node,
            "CREATE TABLE test_experimental_revert (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0",
        )

        # And so is any other EXPERIMENTAL setting
        assert_experimental_change_is_blocked(
            node,
            "CREATE TABLE test_experimental_server_level (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1",
        )

        node.query("DROP TABLE IF EXISTS test_experimental_revert")
        node.query("DROP TABLE IF EXISTS test_experimental_server_level")
        node.query("DROP TABLE IF EXISTS test_server_level_settings")

    # The server also restarts at the strictest tier
    node.restart_clickhouse()
    assert "1" == node.query("SELECT 1").strip()

    set_feature_tier(node, "2", "1", feature_tier_1_path)


def test_altering_unrelated_setting_after_tightening_tier(start_cluster):
    # Table created at tier 0 with an EXPERIMENTAL override must stay alterable for unrelated
    # PRODUCTION settings once the tier is tightened
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_unrelated_alter")
    instance.query(
        "CREATE TABLE test_unrelated_alter (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 1"
    )

    set_feature_tier(instance, "0", "1")

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER TABLE test_unrelated_alter MODIFY SETTING {MERGE_TREE_PRODUCTION_SETTING} = 999999999"
    )
    assert output == ""
    assert error == ""

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_unrelated_alter'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG in output
    assert MERGE_TREE_PRODUCTION_SETTING in output

    set_feature_tier(instance, "1", "0")
    instance.query("DROP TABLE IF EXISTS test_unrelated_alter")


def test_reset_setting_bypassing_feature_tier(start_cluster):
    # RESET SETTING must be checked the same way as MODIFY SETTING to the default value
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_reset_bypass")
    instance.query(
        "CREATE TABLE test_reset_bypass (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 1"
    )

    set_feature_tier(instance, "0", "1")

    assert_experimental_change_is_blocked(
        instance,
        f"ALTER TABLE test_reset_bypass MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0",
    )

    assert_experimental_change_is_blocked(
        instance,
        f"ALTER TABLE test_reset_bypass RESET SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG}",
    )

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_reset_bypass'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG in output

    set_feature_tier(instance, "1", "0")
    instance.query("DROP TABLE IF EXISTS test_reset_bypass")


def test_alter_preserves_aliased_merge_tree_setting(start_cluster):
    # Not tier-specific: an unrelated ALTER must not silently reset a setting that was set through an
    # alias, regardless of allow_feature_tier. `enable_block_number_column` is the only aliased
    # MergeTree setting today (alias `allow_experimental_block_number_column`); if it's renamed, use
    # whatever DECLARE_WITH_ALIAS MergeTree setting exists then
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_alias_preserved")
    instance.query(
        "CREATE TABLE test_alias_preserved (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_ALIASED_SETTING} = 1, enable_block_offset_column = 1"
    )

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER TABLE test_alias_preserved MODIFY SETTING {MERGE_TREE_PRODUCTION_SETTING} = 999999999"
    )
    assert output == ""
    assert error == ""

    # `enable_block_number_column` must still read as enabled: this setting throws otherwise
    output, error = instance.query_and_get_answer_with_error(
        "ALTER TABLE test_alias_preserved MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset'"
    )
    assert output == ""
    assert error == ""

    instance.query("DROP TABLE IF EXISTS test_alias_preserved")


def test_attach_table_with_experimental_merge_tree_setting(start_cluster):
    # A full-definition ATTACH states its settings itself, so it is user input like CREATE is. The short
    # form replays the definition stored on this server and must keep working
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_attach_experimental")
    instance.query(
        "CREATE TABLE test_attach_experimental (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("DETACH TABLE test_attach_experimental")

    set_feature_tier(instance, "0", "1")

    # An `Atomic` database only accepts a full definition together with the table's UUID
    assert_experimental_change_is_blocked(
        instance,
        "ATTACH TABLE test_attach_experimental_new UUID '5b5c1c0e-0e1d-4f0a-9d7f-6b7a4a2f1c11' "
        f"(a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1",
    )

    # The table detached above is attached back from its stored definition
    output, error = instance.query_and_get_answer_with_error(
        "ATTACH TABLE test_attach_experimental"
    )
    assert output == ""
    assert error == ""

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_attach_experimental'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING in output

    set_feature_tier(instance, "1", "0")
    instance.query("DROP TABLE IF EXISTS test_attach_experimental")


def test_custom_settings_belong_to_no_tier(start_cluster):
    # A custom setting is not a feature of the server, so no value of `allow_feature_tier` restricts it,
    # including the first time it is set, when the session does not know the name yet
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=["user_with_custom_setting"])

    for tier in ["0", "1", "2", "3"]:
        if tier != "0":
            set_feature_tier(instance, str(int(tier) - 1), tier)
        assert tier == get_current_tier_value(instance)

        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {CUSTOM_SETTING} = 1"
        )
        assert output.strip() == "1"
        assert error == ""

        output, error = instance.query_and_get_answer_with_error(
            "CREATE USER OR REPLACE user_with_custom_setting IDENTIFIED WITH no_password "
            f"SETTINGS {CUSTOM_SETTING} = 1"
        )
        assert output == ""
        assert error == ""

    set_feature_tier(instance, "3", "0")
    drop_entities(instance, users=["user_with_custom_setting"])


def test_merge_tree_constraint_applies_to_the_alias_of_the_setting(start_cluster):
    # A constraint on a `MergeTree` setting is stored under the canonical name. Writing the setting through
    # its alias is writing the same setting, so the constraint has to apply to it as well
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    drop_entities(
        instance,
        users=["user_with_const_constraint"],
        profiles=["profile_with_const_constraint"],
    )
    instance.query(
        f"CREATE SETTINGS PROFILE profile_with_const_constraint SETTINGS {canonical} CONST"
    )
    instance.query(
        "CREATE USER user_with_const_constraint IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_const_constraint'"
    )

    for name in [canonical, alias]:
        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {name} = 1", user="user_with_const_constraint"
        )
        assert output == ""
        assert "should not be changed" in error, name

    drop_entities(
        instance,
        users=["user_with_const_constraint"],
        profiles=["profile_with_const_constraint"],
    )


def test_merge_tree_setting_is_the_same_setting_when_stored_under_the_alias(
    start_cluster,
):
    # The mirror of the test below: the value is stored under the alias, and the canonical name has to find
    # it. Neither name is privileged, so whichever one a profile used, the other reads and resets the same
    # setting
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    drop_entities(
        instance,
        users=["user_with_alias_stored_setting"],
        profiles=["profile_with_alias_stored_setting"],
    )
    instance.query(
        f"CREATE SETTINGS PROFILE profile_with_alias_stored_setting SETTINGS {alias} = 1 CONST"
    )
    instance.query(
        "CREATE USER user_with_alias_stored_setting IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_alias_stored_setting'"
    )

    for name in [alias, canonical]:
        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {name} = 1", user="user_with_alias_stored_setting"
        )
        assert output.strip() == "1", name
        assert error == "", name

    for name in [alias, canonical]:
        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {name} = 0", user="user_with_alias_stored_setting"
        )
        assert output == ""
        assert "should not be changed" in error, name

    drop_entities(
        instance,
        users=["user_with_alias_stored_setting"],
        profiles=["profile_with_alias_stored_setting"],
    )


def test_merge_tree_setting_is_the_same_setting_under_either_name(start_cluster):
    # A `merge_tree_`-prefixed setting is carried through `Settings` as a custom setting, so its value is
    # stored under the spelling that wrote it and without its declared type. Writing the value it already
    # has changes nothing, whichever of its names is used, and writing a different value is still a change
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    drop_entities(
        instance,
        users=["user_with_const_aliased_setting"],
        profiles=["profile_with_const_aliased_setting"],
    )
    instance.query(
        f"CREATE SETTINGS PROFILE profile_with_const_aliased_setting SETTINGS {canonical} = 1 CONST"
    )
    instance.query(
        "CREATE USER user_with_const_aliased_setting IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_const_aliased_setting'"
    )

    # Re-stating the value it already has is not a change, under either name
    for name in [canonical, alias]:
        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {name} = 1", user="user_with_const_aliased_setting"
        )
        assert output.strip() == "1", name
        assert error == "", name

    # Writing a different value is a change, and `CONST` still refuses it under either name
    for name in [canonical, alias]:
        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {name} = 0", user="user_with_const_aliased_setting"
        )
        assert output == ""
        assert "should not be changed" in error, name

    drop_entities(
        instance,
        users=["user_with_const_aliased_setting"],
        profiles=["profile_with_const_aliased_setting"],
    )


def test_both_names_of_a_merge_tree_setting_hold_one_value(start_cluster):
    # A value is stored under the canonical name of the setting, so the two names of one setting cannot
    # end up holding two values. A profile stating both is the same as stating the last one twice.
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    drop_entities(
        instance,
        users=["user_with_both_names"],
        profiles=["profile_with_both_names"],
    )
    instance.query(
        f"CREATE SETTINGS PROFILE profile_with_both_names SETTINGS {canonical} = 1, {alias} = 0"
    )
    instance.query(
        "CREATE USER user_with_both_names IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_both_names'"
    )

    output = instance.query(
        f"SELECT getSetting('{canonical}'), getSetting('{alias}')",
        user="user_with_both_names",
    )
    assert output.split() == ["0", "0"], output

    # The name a query writes does not matter either
    output = instance.query(
        f"SELECT getSetting('{canonical}'), getSetting('{alias}') SETTINGS {alias} = 1",
        user="user_with_both_names",
    )
    assert output.split() == ["1", "1"], output

    drop_entities(
        instance,
        users=["user_with_both_names"],
        profiles=["profile_with_both_names"],
    )


def test_dropping_a_merge_tree_setting_from_a_profile_under_either_name(start_cluster):
    # A profile holds one element for one setting, so dropping it works under the name that wrote it
    # and under its other name alike
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    for stated, dropped in [(alias, canonical), (canonical, alias)]:
        drop_entities(instance, profiles=["profile_dropping_either_name"])
        instance.query(
            f"CREATE SETTINGS PROFILE profile_dropping_either_name SETTINGS {stated} = 1"
        )
        instance.query(
            f"ALTER SETTINGS PROFILE profile_dropping_either_name DROP SETTING {dropped}"
        )
        output = instance.query(
            "SHOW CREATE SETTINGS PROFILE profile_dropping_either_name"
        )
        assert "block_number_column" not in output, output

    drop_entities(instance, profiles=["profile_dropping_either_name"])


def test_alter_replays_on_a_replica_that_would_not_have_allowed_it(start_cluster):
    # A `Replicated` database runs the ALTER again on every other replica. The replica that took the query
    # from the user is the one that decides whether it is allowed; the others must apply it even when their
    # own `allow_feature_tier` is stricter, or the database replication queue stops and the replicas end up
    # with different table metadata.
    database = "database_of_replicas_with_different_tiers"
    table = f"{database}.table_altered_on_one_replica"

    assert "0" == get_current_tier_value(permissive_replica)
    assert "1" == get_current_tier_value(strict_replica)

    for replica_name, replica in [("one", permissive_replica), ("two", strict_replica)]:
        replica.query(f"DROP DATABASE IF EXISTS {database} SYNC")
        replica.query(
            f"CREATE DATABASE {database} ENGINE = Replicated('/test/{database}', 'shard_one', '{replica_name}')"
        )

    permissive_replica.query(
        f"CREATE TABLE {table} (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"
    )

    # Allowed on this replica because it allows every tier. A `Replicated` database reports one row per
    # replica, and both must say `OK`: the stricter replica has to accept the change too.
    output, error = permissive_replica.query_and_get_answer_with_error(
        f"ALTER TABLE {table} MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
    )
    assert error == ""
    statuses = [line.split("\t")[2] for line in output.strip().split("\n")]
    assert statuses == ["OK", "OK"], output

    # The stricter replica applies the same ALTER. `SYSTEM SYNC DATABASE REPLICA` fails if the entry did
    # not go through, so reaching the assertion already means the queue is not stuck.
    strict_replica.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
    assert MERGE_TREE_EXPERIMENTAL_SETTING in strict_replica.query(
        f"SHOW CREATE TABLE {table}"
    )

    # The stricter replica still refuses the same ALTER when a user sends it there directly
    assert_experimental_change_is_blocked(
        strict_replica,
        f"ALTER TABLE {table} MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING} = 0",
    )

    for replica in [permissive_replica, strict_replica]:
        replica.query(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_reset_of_a_merge_tree_setting_cannot_escape_its_constraint(start_cluster):
    # `SET ... = DEFAULT` drops the value, so it has to be checked against the real default of the
    # setting, the same way a reset of a plain setting is. The default of this one is 8192, below the
    # minimum the profile declares, so the reset has to be refused. Otherwise the constraint is escaped
    # by dropping the setting instead of by writing a value it forbids.
    node = instance_with_merge_tree_constraint
    name = MERGE_TREE_SETTING_WITH_A_FORBIDDEN_DEFAULT

    assert MERGE_TREE_FORBIDDEN_DEFAULT_MIN == int(
        node.query(f"SELECT getSetting('{name}')").strip()
    )

    # Writing a value below the minimum is refused
    output, error = node.query_and_get_answer_with_error(
        f"SELECT 1 SETTINGS {name} = 1024"
    )
    assert output == ""
    assert "shouldn't be less than" in error, error

    # Resetting to the default, which is below the same minimum, has to be refused too
    output, error = node.query_and_get_answer_with_error(f"SET {name} = DEFAULT")
    assert output == ""
    assert "shouldn't be less than" in error, error

    # A setting whose default satisfies the constraint can still be reset
    output, error = node.query_and_get_answer_with_error(
        f"SET {MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} = DEFAULT"
    )
    assert output == ""
    assert error == "", error


def test_old_syntax_index_granularity_cannot_escape_its_constraint(start_cluster):
    # The old syntax states `index_granularity` as an engine argument, and a full-definition `ATTACH`
    # is user input like `CREATE`. Either way it is a fresh definition, so the constraint applies.
    node = instance_with_merge_tree_constraint
    old_syntax = {"allow_deprecated_syntax_for_merge_tree": 1}
    node.query("DROP TABLE IF EXISTS test_old_syntax_granularity")

    output, error = node.query_and_get_answer_with_error(
        "CREATE TABLE test_old_syntax_granularity (d Date, k UInt64) "
        "ENGINE = MergeTree(d, k, 1024)",
        settings=old_syntax,
    )
    assert output == ""
    assert "shouldn't be less than" in error, error

    output, error = node.query_and_get_answer_with_error(
        "ATTACH TABLE test_old_syntax_granularity_attached "
        "UUID '5b5c1c0e-0e1d-4f0a-9d7f-6b7a4a2f1c21' (d Date, k UInt64) "
        "ENGINE = MergeTree(d, k, 1024)",
        settings=old_syntax,
    )
    assert output == ""
    assert "shouldn't be less than" in error, error

    # A value the constraint allows is accepted
    output, error = node.query_and_get_answer_with_error(
        "CREATE TABLE test_old_syntax_granularity (d Date, k UInt64) "
        f"ENGINE = MergeTree(d, k, {MERGE_TREE_FORBIDDEN_DEFAULT_MIN})",
        settings=old_syntax,
    )
    assert output == ""
    assert error == "", error

    node.query("DROP TABLE IF EXISTS test_old_syntax_granularity")


def test_projection_settings_of_a_freshly_attached_table_are_checked(start_cluster):
    # A projection's `WITH SETTINGS` is part of the definition, so a full-definition `ATTACH` cannot
    # carry settings that `CREATE` would refuse
    node = instance_with_merge_tree_constraint
    node.query("DROP TABLE IF EXISTS test_projection_settings")

    definition = (
        "(a UInt64, PROJECTION p (SELECT a ORDER BY a) WITH SETTINGS (index_granularity = 1024)) "
        "ENGINE = MergeTree ORDER BY a"
    )

    output, error = node.query_and_get_answer_with_error(
        f"CREATE TABLE test_projection_settings {definition}"
    )
    assert output == ""
    assert "shouldn't be less than" in error, error

    output, error = node.query_and_get_answer_with_error(
        "ATTACH TABLE test_projection_settings "
        f"UUID '5b5c1c0e-0e1d-4f0a-9d7f-6b7a4a2f1c22' {definition}"
    )
    assert output == ""
    assert "shouldn't be less than" in error, error

    # A setting a projection does not accept at all is refused the same way
    output, error = node.query_and_get_answer_with_error(
        "ATTACH TABLE test_projection_settings "
        "UUID '5b5c1c0e-0e1d-4f0a-9d7f-6b7a4a2f1c23' "
        "(a UInt64, PROJECTION p (SELECT a ORDER BY a) WITH SETTINGS (merge_max_block_size = 1024)) "
        "ENGINE = MergeTree ORDER BY a"
    )
    assert output == ""
    assert "is not allowed for projections" in error, error

    node.query("DROP TABLE IF EXISTS test_projection_settings")


# `allow_feature_tier` enforcement for access entities. A statement is refused when it changes, for some
# user, the value in effect of a setting of a disabled tier, whether or not the statement names the setting.


def test_granting_a_role_that_a_profile_is_assigned_to(start_cluster):
    users, roles, profiles = ["tier_g1"], ["tier_carrier_role"], ["tier_carrier_profile"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users, roles, profiles)

    instance.query("CREATE USER tier_g1 IDENTIFIED WITH no_password")
    instance.query("CREATE ROLE tier_carrier_role")
    instance.query(
        f"CREATE SETTINGS PROFILE tier_carrier_profile SETTINGS {EXPERIMENTAL_SETTING} = 1 TO tier_carrier_role"
    )
    assert read_experimental_setting(instance, "tier_g1") == "0"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, "GRANT tier_carrier_role TO tier_g1"
            )
            assert read_experimental_setting(instance, "tier_g1") == "0"
    finally:
        drop_entities(instance, users, roles, profiles)


def test_dropping_a_settings_profile_a_user_uses(start_cluster):
    users, profiles = ["tier_g2"], ["tier_droppable_profile"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, profiles=profiles)

    instance.query("CREATE USER tier_g2 IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE SETTINGS PROFILE tier_droppable_profile SETTINGS {EXPERIMENTAL_SETTING} = 1 TO tier_g2"
    )
    assert read_experimental_setting(instance, "tier_g2") == "1"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, "DROP SETTINGS PROFILE tier_droppable_profile"
            )
            assert read_experimental_setting(instance, "tier_g2") == "1"
    finally:
        drop_entities(instance, users=users, profiles=profiles)


def test_dropping_a_role_that_carries_a_setting(start_cluster):
    users, roles = ["tier_g4"], ["tier_droppable_role"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, roles=roles)

    instance.query("CREATE USER tier_g4 IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE ROLE tier_droppable_role SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT tier_droppable_role TO tier_g4")
    assert read_experimental_setting(instance, "tier_g4") == "1"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, "DROP ROLE tier_droppable_role"
            )
            assert read_experimental_setting(instance, "tier_g4") == "1"
    finally:
        drop_entities(instance, users=users, roles=roles)


def test_replacing_a_user_discarding_a_granted_role(start_cluster):
    users, roles = ["tier_g5"], ["tier_replaced_away_role"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, roles=roles)

    instance.query(
        f"CREATE ROLE tier_replaced_away_role SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE USER tier_g5 IDENTIFIED WITH no_password")
    instance.query("GRANT tier_replaced_away_role TO tier_g5")
    assert read_experimental_setting(instance, "tier_g5") == "1"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance,
                "CREATE USER OR REPLACE tier_g5 IDENTIFIED WITH no_password",
            )
            assert read_experimental_setting(instance, "tier_g5") == "1"
    finally:
        drop_entities(instance, users=users, roles=roles)


def test_dropping_a_setting_from_a_user(start_cluster):
    users = ["tier_g6"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users)

    instance.query(
        f"CREATE USER tier_g6 IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert read_experimental_setting(instance, "tier_g6") == "1"

    try:
        with feature_tier(instance, "1"):
            for statement in [
                f"ALTER USER tier_g6 DROP SETTING {EXPERIMENTAL_SETTING}",
                "ALTER USER tier_g6 DROP ALL SETTINGS",
                "ALTER USER tier_g6 SETTINGS NONE",
                "CREATE USER OR REPLACE tier_g6 IDENTIFIED WITH no_password",
            ]:
                assert_experimental_change_is_blocked(instance, statement)
                assert read_experimental_setting(instance, "tier_g6") == "1"
    finally:
        drop_entities(instance, users=users)


def test_revoking_a_role_that_carries_a_setting(start_cluster):
    users, roles = ["tier_g7"], ["tier_revocable_role"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, roles=roles)

    instance.query(
        f"CREATE ROLE tier_revocable_role SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE USER tier_g7 IDENTIFIED WITH no_password")
    instance.query("GRANT tier_revocable_role TO tier_g7")
    assert read_experimental_setting(instance, "tier_g7") == "1"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, "REVOKE tier_revocable_role FROM tier_g7"
            )
            assert read_experimental_setting(instance, "tier_g7") == "1"
    finally:
        drop_entities(instance, users=users, roles=roles)


def test_making_a_granted_role_default(start_cluster):
    users, roles = ["tier_g8"], ["tier_not_default_role"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, roles=roles)

    instance.query(
        f"CREATE ROLE tier_not_default_role SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE USER tier_g8 IDENTIFIED WITH no_password DEFAULT ROLE NONE")
    instance.query("GRANT tier_not_default_role TO tier_g8")
    # A granted role that is not a default role carries nothing
    assert read_experimental_setting(instance, "tier_g8") == "0"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, "SET DEFAULT ROLE ALL TO tier_g8"
            )
            assert read_experimental_setting(instance, "tier_g8") == "0"
    finally:
        drop_entities(instance, users=users, roles=roles)


def test_assigning_a_profile_to_a_user(start_cluster):
    users, profiles = ["tier_g9"], ["tier_assignable_profile"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, profiles=profiles)

    instance.query("CREATE USER tier_g9 IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE SETTINGS PROFILE tier_assignable_profile SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert read_experimental_setting(instance, "tier_g9") == "0"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance,
                "ALTER SETTINGS PROFILE tier_assignable_profile TO tier_g9",
            )
            assert read_experimental_setting(instance, "tier_g9") == "0"
    finally:
        drop_entities(instance, users=users, profiles=profiles)


def test_a_setting_shadowed_by_a_dependent_role(start_cluster):
    # `tier_child` sets the setting to 0 and `tier_parent` to 1, and `tier_child` is granted to
    # `tier_parent`, so the user reads 0. Dropping the setting from `tier_child` does not change what
    # `tier_child` alone resolves to, but it moves the user's value from 0 to 1
    users, roles = ["tier_g10"], ["tier_parent", "tier_child"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users, roles=roles)

    instance.query(f"CREATE ROLE tier_child SETTINGS {EXPERIMENTAL_SETTING} = 0")
    instance.query(f"CREATE ROLE tier_parent SETTINGS {EXPERIMENTAL_SETTING} = 1")
    instance.query("GRANT tier_child TO tier_parent")
    instance.query("CREATE USER tier_g10 IDENTIFIED WITH no_password")
    instance.query("GRANT tier_parent TO tier_g10")
    assert read_experimental_setting(instance, "tier_g10") == "0"

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance,
                f"ALTER ROLE tier_child DROP SETTING {EXPERIMENTAL_SETTING}",
            )
            assert read_experimental_setting(instance, "tier_g10") == "0"
    finally:
        drop_entities(instance, users=users, roles=roles)


def test_restating_the_value_a_user_already_has(start_cluster):
    # The statement changes no setting for `tier_target`, so it is allowed even though the administrator
    # running it has a different value in their own session
    users = ["tier_admin", "tier_target"]
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users=users)

    instance.query("CREATE USER tier_admin IDENTIFIED WITH no_password")
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO tier_admin")
    instance.query(
        f"CREATE USER tier_target IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert read_experimental_setting(instance, "tier_admin") == "0"
    assert read_experimental_setting(instance, "tier_target") == "1"

    try:
        with feature_tier(instance, "1"):
            output, error = instance.query_and_get_answer_with_error(
                f"ALTER USER tier_target SETTINGS {EXPERIMENTAL_SETTING} = 1",
                user="tier_admin",
            )
            assert error == "", error
            assert read_experimental_setting(instance, "tier_target") == "1"
    finally:
        drop_entities(instance, users=users)


def test_unrelated_access_entity_statements_are_allowed(start_cluster):
    # Only the settings in effect decide the refusal: statements that change no setting keep working
    users, roles, profiles = (
        ["tier_g11"],
        ["tier_plain_role"],
        ["tier_unused_profile"],
    )
    assert "0" == get_current_tier_value(instance)
    drop_entities(instance, users, roles, profiles)

    try:
        with feature_tier(instance, "1"):
            for statement in [
                "CREATE USER tier_g11 IDENTIFIED WITH no_password",
                "CREATE ROLE tier_plain_role",
                "GRANT SELECT ON system.* TO tier_plain_role",
                "GRANT tier_plain_role TO tier_g11",
                f"CREATE SETTINGS PROFILE tier_unused_profile SETTINGS {MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE} = 1073741824 TO tier_g11",
                "ALTER USER tier_g11 RENAME TO tier_g11",
                "REVOKE tier_plain_role FROM tier_g11",
                "DROP ROLE tier_plain_role",
                "DROP SETTINGS PROFILE tier_unused_profile",
                "DROP USER tier_g11",
            ]:
                output, error = instance.query_and_get_answer_with_error(statement)
                assert error == "", statement + ": " + error
    finally:
        drop_entities(instance, users, roles, profiles)


def test_feature_tier_is_enforced_in_named_access_storage(start_cluster):
    users = ["tier_storage_alter", "tier_storage_role_user"]
    roles = ["tier_storage_role"]
    all_users = users + ["tier_storage_create"]

    drop_entities(instance, users=all_users, roles=roles, storage="memory")

    instance.query(
        f"CREATE USER tier_storage_alter IN memory IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(
        "CREATE USER tier_storage_role_user IN memory IDENTIFIED WITH no_password"
    )
    instance.query(
        f"CREATE ROLE tier_storage_role IN memory SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT tier_storage_role TO tier_storage_role_user")

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance,
                f"CREATE USER tier_storage_create IN memory IDENTIFIED WITH no_password "
                f"SETTINGS {EXPERIMENTAL_SETTING} = 1",
            )
            assert_experimental_change_is_blocked(
                instance,
                f"ALTER USER tier_storage_alter IN memory DROP SETTING {EXPERIMENTAL_SETTING}",
            )
            assert_experimental_change_is_blocked(
                instance, "DROP ROLE tier_storage_role FROM memory"
            )

            assert (
                instance.query(
                    "SELECT count() FROM system.users WHERE name = 'tier_storage_create'"
                ).strip()
                == "0"
            )
            assert read_experimental_setting(instance, "tier_storage_alter") == "1"
            assert read_experimental_setting(instance, "tier_storage_role_user") == "1"
    finally:
        drop_entities(instance, users=all_users, roles=roles, storage="memory")


def test_renaming_a_user_does_not_hide_a_settings_change(start_cluster):
    old_name = "tier_rename_old"
    new_name = "tier_rename_new"
    drop_entities(instance, users=[old_name, new_name])

    instance.query(
        f"CREATE USER {old_name} IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance,
                f"ALTER USER {old_name} RENAME TO {new_name} "
                f"DROP SETTING {EXPERIMENTAL_SETTING}",
            )
            assert read_experimental_setting(instance, old_name) == "1"
            assert (
                instance.query(
                    f"SELECT count() FROM system.users WHERE name = '{new_name}'"
                ).strip()
                == "0"
            )
    finally:
        drop_entities(instance, users=[old_name, new_name])


def test_dropping_an_explicit_default_value_is_allowed(start_cluster):
    user = "tier_explicit_default"
    drop_entities(instance, users=[user])

    instance.query(
        f"CREATE USER {user} IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 0"
    )

    try:
        with feature_tier(instance, "1"):
            output, error = instance.query_and_get_answer_with_error(
                f"ALTER USER {user} DROP SETTING {EXPERIMENTAL_SETTING}"
            )
            assert output == ""
            assert error == "", error
            assert read_experimental_setting(instance, user) == "0"
    finally:
        drop_entities(instance, users=[user])


def test_create_user_if_not_exists_is_a_no_op(start_cluster):
    user = "tier_if_not_exists"
    drop_entities(instance, users=[user])

    instance.query(
        f"CREATE USER {user} IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    try:
        with feature_tier(instance, "1"):
            output, error = instance.query_and_get_answer_with_error(
                f"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH no_password"
            )
            assert output == ""
            assert error == "", error
            assert read_experimental_setting(instance, user) == "1"
    finally:
        drop_entities(instance, users=[user])


def test_overlapping_settings_profiles_use_the_effective_precedence(start_cluster):
    user = "tier_overlapping_profiles"
    profile_zero = "tier_profile_zero"
    profile_one = "tier_profile_one"
    profiles = [profile_zero, profile_one]
    drop_entities(instance, users=[user], profiles=profiles)

    instance.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE SETTINGS PROFILE {profile_zero} SETTINGS {EXPERIMENTAL_SETTING} = 0 TO {user}"
    )
    instance.query(
        f"CREATE SETTINGS PROFILE {profile_one} SETTINGS {EXPERIMENTAL_SETTING} = 1 TO {user}"
    )
    value_before = read_experimental_setting(instance, user)
    effective_profile = profile_one if value_before == "1" else profile_zero

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, f"DROP SETTINGS PROFILE {effective_profile}"
            )
            assert read_experimental_setting(instance, user) == value_before
    finally:
        drop_entities(instance, users=[user], profiles=profiles)


def test_concurrent_access_changes_cannot_compose_a_restricted_setting(start_cluster):
    user = "tier_concurrent_user"
    role = "tier_concurrent_role"
    drop_entities(instance, users=[user], roles=[role])

    instance.query(
        f"CREATE ROLE {role} SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(
        f"CREATE USER {user} IDENTIFIED WITH no_password DEFAULT ROLE NONE"
    )

    results = {}
    grant_thread = None
    default_role_thread = None
    try:
        with feature_tier(instance, "1"):
            instance.query(
                f"SYSTEM ENABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
            )
            grant_thread = threading.Thread(
                target=lambda: results.update(
                    grant=instance.query_and_get_answer_with_error(
                        f"GRANT {role} TO {user}"
                    )
                )
            )
            grant_thread.start()
            instance.query(
                f"SYSTEM WAIT FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT} PAUSE",
                timeout=60,
            )

            default_role_thread = threading.Thread(
                target=lambda: results.update(
                    default_role=instance.query_and_get_answer_with_error(
                        f"SET DEFAULT ROLE ALL TO {user}"
                    )
                )
            )
            default_role_thread.start()
            instance.query(
                f"SYSTEM NOTIFY FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
            )

            grant_thread.join(timeout=60)
            default_role_thread.join(timeout=60)
            assert not grant_thread.is_alive()
            assert not default_role_thread.is_alive()
            assert results["grant"] == ("", "")
            assert results["default_role"][0] == ""
            assert EXPERIMENTAL_BLOCKED in results["default_role"][1]
            assert read_experimental_setting(instance, user) == "0"
    finally:
        instance.query(
            f"SYSTEM DISABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
        )
        if grant_thread is not None:
            grant_thread.join(timeout=60)
        if default_role_thread is not None:
            default_role_thread.join(timeout=60)
        drop_entities(instance, users=[user], roles=[role])


def test_moving_a_role_preserves_the_effective_setting(start_cluster):
    user = "tier_move_user"
    role = "tier_move_role"
    drop_entities(instance, users=[user], roles=[role], storage="memory")
    drop_entities(instance, roles=[role], storage="local_directory")

    instance.query(f"CREATE USER {user} IN memory IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE ROLE {role} IN memory SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(f"GRANT {role} TO {user}")
    assert read_experimental_setting(instance, user) == "1"

    try:
        with feature_tier(instance, "1"):
            output, error = instance.query_and_get_answer_with_error(
                f"MOVE ROLE {role} TO local_directory"
            )
            assert output == ""
            assert error == "", error
            assert read_experimental_setting(instance, user) == "1"
            assert (
                instance.query(
                    f"SELECT storage FROM system.roles WHERE name = '{role}'"
                ).strip()
                == "local_directory"
            )
    finally:
        drop_entities(instance, users=[user], roles=[role], storage="memory")
        drop_entities(instance, roles=[role], storage="local_directory")


def test_move_rolls_back_entities_removed_before_failure(start_cluster):
    role = "tier_move_rollback_role"
    drop_entities(instance, roles=[role], storage="memory")
    drop_entities(instance, roles=[role], storage="local_directory")
    instance.query(f"CREATE ROLE {role} IN memory")

    try:
        output, error = instance.query_and_get_answer_with_error(
            f"MOVE ROLE {role}, {role} TO local_directory"
        )
        assert output == ""
        assert "After successfully removing 1/2" in error, error
        assert (
            instance.query(
                f"SELECT storage FROM system.roles WHERE name = '{role}'"
            ).strip()
            == "memory"
        )
    finally:
        drop_entities(instance, roles=[role], storage="memory")
        drop_entities(instance, roles=[role], storage="local_directory")


def test_create_if_not_exists_notifies_when_shadowing_config_user(start_cluster):
    user = "tier_config_user"
    drop_entities(instance, users=[user], storage="local_directory")
    assert read_experimental_setting(instance, user) == "0"

    try:
        instance.query(
            f"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH no_password "
            f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
        )
        storages = instance.query(
            f"SELECT storage FROM system.users WHERE name = '{user}' ORDER BY storage"
        ).splitlines()
        assert storages == ["local_directory", "users_xml"]
        assert read_experimental_setting(instance, user) == "1"
    finally:
        drop_entities(instance, users=[user], storage="local_directory")


def test_named_storage_collision_is_checked_before_batch_insert(start_cluster):
    new_user = "tier_batch_new_user"
    drop_entities(instance, users=[new_user], storage="memory")

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER {new_user}, tier_config_user IN memory IDENTIFIED WITH no_password"
    )
    assert output == ""
    assert "already exists" in error, error
    assert (
        instance.query(
            f"SELECT count() FROM system.users WHERE name = '{new_user}'"
        ).strip()
        == "0"
    )


def test_const_constraint_is_sticky_when_previous_constraints_are_kept(start_cluster):
    node = instance_with_legacy_constraints
    user = "tier_legacy_constraint_user"
    base_profile = "tier_legacy_constraint_base"
    profile = "tier_legacy_constraint_profile"
    profiles = [base_profile, profile]
    drop_entities(node, users=[user], profiles=profiles)

    node.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
    node.query(
        f"CREATE SETTINGS PROFILE {base_profile} "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 0 CONST"
    )
    node.query(
        f"CREATE SETTINGS PROFILE {profile} SETTINGS INHERIT {base_profile}, "
        f"{EXPERIMENTAL_SETTING} = 0 WRITABLE TO {user}"
    )

    try:
        with feature_tier(node, "1"):
            assert_experimental_change_is_blocked(
                node,
                f"ALTER SETTINGS PROFILE {profile} DROP PROFILES {base_profile}",
            )
    finally:
        drop_entities(node, users=[user], profiles=profiles)


def test_restore_access_entities_checks_feature_tier(start_cluster):
    user = "tier_restored_user"
    backup_name = f"tier_restore_{uuid.uuid4().hex}"
    backup = f"Disk('backups', '{backup_name}')"
    drop_entities(instance, users=[user])

    instance.query(
        f"CREATE USER {user} IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(f"BACKUP TABLE system.users TO {backup}")
    drop_entities(instance, users=[user])

    try:
        with feature_tier(instance, "1"):
            assert_experimental_change_is_blocked(
                instance, f"RESTORE TABLE system.users FROM {backup}"
            )
            assert (
                instance.query(
                    f"SELECT count() FROM system.users WHERE name = '{user}'"
                ).strip()
                == "0"
            )
    finally:
        drop_entities(instance, users=[user])


def test_replicated_update_reapplies_after_version_conflict(start_cluster):
    user = "tier_replicated_cas_user"
    grant_thread = None
    results = {}
    drop_entities(permissive_replica, users=[user])
    permissive_replica.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
    strict_replica.query_with_retry(
        f"SELECT count() FROM system.users WHERE name = '{user}'",
        check_callback=lambda value: value.strip() == "1",
    )

    try:
        strict_replica.query(
            f"SYSTEM ENABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
        )
        grant_thread = threading.Thread(
            target=lambda: results.update(
                grant=strict_replica.query_and_get_answer_with_error(
                    f"GRANT SELECT ON tier_cas_select.* TO {user}"
                )
            )
        )
        grant_thread.start()
        strict_replica.query(
            f"SYSTEM WAIT FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT} PAUSE",
            timeout=60,
        )

        permissive_replica.query(f"GRANT INSERT ON tier_cas_insert.* TO {user}")
        strict_replica.query(
            f"SYSTEM NOTIFY FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
        )
        grant_thread.join(timeout=60)
        assert not grant_thread.is_alive()
        assert results["grant"] == ("", "")

        for node in [permissive_replica, strict_replica]:
            grants = node.query_with_retry(
                f"SHOW GRANTS FOR {user}",
                check_callback=lambda value: "GRANT SELECT ON tier_cas_select.*"
                in value
                and "GRANT INSERT ON tier_cas_insert.*" in value,
            )
            assert "GRANT SELECT ON tier_cas_select.*" in grants
            assert "GRANT INSERT ON tier_cas_insert.*" in grants
    finally:
        strict_replica.query(
            f"SYSTEM NOTIFY FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}",
            ignore_error=True,
        )
        strict_replica.query(
            f"SYSTEM DISABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}",
            ignore_error=True,
        )
        if grant_thread is not None:
            grant_thread.join(timeout=60)
        drop_entities(permissive_replica, users=[user])


def test_replicas_cannot_commit_two_halves_of_restricted_change(start_cluster):
    user = "tier_replicated_graph_user"
    role = "tier_replicated_graph_role"
    profile = "tier_replicated_graph_profile"
    grant_thread = None
    results = {}
    drop_entities(
        permissive_replica, users=[user], roles=[role], profiles=[profile]
    )

    permissive_replica.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
    permissive_replica.query(f"CREATE ROLE {role}")
    permissive_replica.query(
        f"CREATE SETTINGS PROFILE {profile} SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    strict_replica.query_with_retry(
        f"SELECT count() FROM system.settings_profiles WHERE name = '{profile}'",
        check_callback=lambda value: value.strip() == "1",
    )

    try:
        with feature_tier(permissive_replica, "1"):
            strict_replica.query(
                f"SYSTEM ENABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
            )
            grant_thread = threading.Thread(
                target=lambda: results.update(
                    grant=strict_replica.query_and_get_answer_with_error(
                        f"GRANT {role} TO {user}"
                    )
                )
            )
            grant_thread.start()
            strict_replica.query(
                f"SYSTEM WAIT FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT} PAUSE",
                timeout=60,
            )

            assert_experimental_change_is_blocked(
                permissive_replica,
                f"ALTER SETTINGS PROFILE {profile} TO {role}",
            )
            strict_replica.query(
                f"SYSTEM NOTIFY FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}"
            )
            grant_thread.join(timeout=60)
            assert not grant_thread.is_alive()
            assert results["grant"] == ("", "")
            strict_replica.query_with_retry(
                f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
                user=user,
                check_callback=lambda value: value.strip() == "0",
            )
    finally:
        strict_replica.query(
            f"SYSTEM NOTIFY FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}",
            ignore_error=True,
        )
        strict_replica.query(
            f"SYSTEM DISABLE FAILPOINT {ACCESS_CONTROL_FEATURE_TIER_FAILPOINT}",
            ignore_error=True,
        )
        if grant_thread is not None:
            grant_thread.join(timeout=60)
        drop_entities(
            permissive_replica, users=[user], roles=[role], profiles=[profile]
        )
