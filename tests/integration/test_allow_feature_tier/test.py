import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/allow_feature_tier.xml",
        "configs/custom_settings_prefix.xml",
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

# Two replicas of one `Replicated` database that disagree on which settings are allowed: the first one
# allows every tier, the second one refuses EXPERIMENTAL settings.
permissive_replica = cluster.add_instance(
    "permissive_replica",
    main_configs=["configs/allow_feature_tier.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
strict_replica = cluster.add_instance(
    "strict_replica",
    main_configs=["configs/allow_feature_tier_1.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

feature_tier_path = "/etc/clickhouse-server/config.d/allow_feature_tier.xml"
feature_tier_1_path = "/etc/clickhouse-server/config.d/allow_feature_tier_1.xml"

# These settings are used as examples of their tier. If one changes tier in the future, please replace
# it with another setting of the same tier. If there is none, feel free to comment out the affected test.
EXPERIMENTAL_SETTING = (
    "allow_experimental_time_series_table"  # also in configs/users.d/users.xml
)
BETA_SETTING = "allow_experimental_lightweight_update"

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


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_current_tier_value(instance):
    query_with_current_tier_value = (
        "SELECT value FROM system.server_settings where name = 'allow_feature_tier'"
    )
    return instance.query(query_with_current_tier_value).strip()


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
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_setting
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental and private preview settings. Beta settings are still allowed.
    instance.replace_in_config(feature_tier_path, "1", "2")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_setting
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental, private preview and beta settings
    instance.replace_in_config(feature_tier_path, "2", "3")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "3" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_setting
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert output == ""
    assert BETA_BLOCKED in error

    # Leave the server as it was
    instance.replace_in_config(feature_tier_path, "3", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)


def test_allow_feature_tier_in_mergetree_settings(start_cluster):
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")

    # Disable experimental settings
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    query_with_experimental_mergetree_setting = f"""
        CREATE TABLE test_experimental (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING}=1;
    """

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_mergetree_setting
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    # Go back
    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)

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
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    instance.restart_clickhouse()

    # After the reboot the table will be there
    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_experimental'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING in output

    # Creating a different table should not be possible
    output, error = instance.query_and_get_answer_with_error(f"""
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING}=1;
    """)
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    # Creating a different table and altering its settings to enable experimental should not be possible either
    output, error = instance.query_and_get_answer_with_error("""
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid);
    """)
    assert output == ""
    assert error == ""

    output, error = instance.query_and_get_answer_with_error(f"""
        ALTER TABLE test_experimental_new MODIFY setting {MERGE_TREE_EXPERIMENTAL_SETTING}=1
    """)
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error
    instance.query("DROP TABLE IF EXISTS test_experimental_new")

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")


def test_allow_feature_tier_in_mergetree_settings_with_old_compatibility(start_cluster):
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")

    # Disable experimental settings
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    basic_merge_tree_query = """
        create table b (a Int64) ENGINE=MergeTree() order by a;
    """

    output, error = instance.query_and_get_answer_with_error(basic_merge_tree_query)
    assert output == ""
    assert error == ""

    # Go back
    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS b")


def test_allow_feature_tier_in_user(start_cluster):
    instance.query("DROP USER IF EXISTS user_experimental")
    assert "0" == get_current_tier_value(instance)

    # Disable experimental settings
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    # Go back to normal and create the user to restart the server and verify it works
    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    assert output == ""
    assert error == ""

    # Default user = 0
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'"
    )
    assert output.strip() == "0"
    assert error == ""

    # New user = 1
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""

    # Change back to block experimental features and restart to confirm everything is working as expected (only new changes are blocked)
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    instance.restart_clickhouse()

    # Default user = 0
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'"
    )
    assert output.strip() == "0"
    assert error == ""

    # New user = 1
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""

    # But note that they can't change the value either
    # 1 - 1 => OK
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=1",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""
    # 1 - 0 => KO
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=0",
        user="user_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_experimental")


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
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'"
    )
    assert output.strip() == "1"
    assert error == ""

    # But it won't be possible to change it
    output, error = instance.query_and_get_answer_with_error(
        f"SELECT 1 SETTINGS {EXPERIMENTAL_SETTING}=0"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

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
        instance.query(
            "DROP SETTINGS PROFILE IF EXISTS profile_with_merge_tree_element"
        )
        instance.query("DROP USER IF EXISTS user_with_merge_tree_element")

    drop_objects()

    for tier in ["1", "2"]:
        instance.replace_in_config(feature_tier_path, "0", tier)
        instance.query("SYSTEM RELOAD CONFIG")
        assert tier == get_current_tier_value(instance)

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
        output, error = instance.query_and_get_answer_with_error(
            "CREATE SETTINGS PROFILE profile_with_experimental_merge_tree_element SETTINGS "
            f"{MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE} = 1"
        )
        assert output == ""
        assert EXPERIMENTAL_BLOCKED in error

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
        instance.replace_in_config(feature_tier_path, tier, "0")
        instance.query("SYSTEM RELOAD CONFIG")
        assert "0" == get_current_tier_value(instance)


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
            node.replace_in_config(feature_tier_1_path, "1", tier)
            node.query("SYSTEM RELOAD CONFIG")
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
        output, error = node.query_and_get_answer_with_error(
            f"ALTER TABLE test_server_level_settings MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0"
        )
        assert output == ""
        assert EXPERIMENTAL_BLOCKED in error

        # Same for `CREATE TABLE`
        output, error = node.query_and_get_answer_with_error(
            "CREATE TABLE test_experimental_revert (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0"
        )
        assert output == ""
        assert EXPERIMENTAL_BLOCKED in error

        # And so is any other EXPERIMENTAL setting
        output, error = node.query_and_get_answer_with_error(
            "CREATE TABLE test_experimental_server_level (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
        )
        assert output == ""
        assert EXPERIMENTAL_BLOCKED in error

        node.query("DROP TABLE IF EXISTS test_experimental_revert")
        node.query("DROP TABLE IF EXISTS test_experimental_server_level")
        node.query("DROP TABLE IF EXISTS test_server_level_settings")

    # The server also restarts at the strictest tier
    node.restart_clickhouse()
    assert "1" == node.query("SELECT 1").strip()

    node.replace_in_config(feature_tier_1_path, "2", "1")
    node.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(node)


def test_altering_unrelated_setting_after_tightening_tier(start_cluster):
    # Table created at tier 0 with an EXPERIMENTAL override must stay alterable for unrelated
    # PRODUCTION settings once the tier is tightened
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_unrelated_alter")
    instance.query(
        "CREATE TABLE test_unrelated_alter (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

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

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_unrelated_alter")


def test_reset_setting_bypassing_feature_tier(start_cluster):
    # RESET SETTING must be checked the same way as MODIFY SETTING to the default value
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_reset_bypass")
    instance.query(
        "CREATE TABLE test_reset_bypass (a UInt64) ENGINE = MergeTree ORDER BY a "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER TABLE test_reset_bypass MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG} = 0"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER TABLE test_reset_bypass RESET SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG}"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_reset_bypass'"
    )
    assert MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG in output

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
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

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    # An `Atomic` database only accepts a full definition together with the table's UUID
    output, error = instance.query_and_get_answer_with_error(
        "ATTACH TABLE test_attach_experimental_new UUID '5b5c1c0e-0e1d-4f0a-9d7f-6b7a4a2f1c11' "
        f"(a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

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

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_attach_experimental")


def test_custom_settings_belong_to_no_tier(start_cluster):
    # A custom setting is not a feature of the server, so no value of `allow_feature_tier` restricts it,
    # including the first time it is set, when the session does not know the name yet
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_with_custom_setting")

    for tier in ["0", "1", "2", "3"]:
        if tier != "0":
            instance.replace_in_config(feature_tier_path, str(int(tier) - 1), tier)
            instance.query("SYSTEM RELOAD CONFIG")
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

    instance.replace_in_config(feature_tier_path, "3", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_with_custom_setting")


def test_merge_tree_constraint_applies_to_the_alias_of_the_setting(start_cluster):
    # A constraint on a `MergeTree` setting is stored under the canonical name. Writing the setting through
    # its alias is writing the same setting, so the constraint has to apply to it as well
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_const_constraint")
    instance.query("DROP USER IF EXISTS user_with_const_constraint")
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

    instance.query("DROP USER IF EXISTS user_with_const_constraint")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_const_constraint")


def test_merge_tree_setting_is_the_same_setting_when_stored_under_the_alias(
    start_cluster,
):
    # The mirror of the test below: the value is stored under the alias, and the canonical name has to find
    # it. Neither name is privileged, so whichever one a profile used, the other reads and resets the same
    # setting
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    instance.query("DROP USER IF EXISTS user_with_alias_stored_setting")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_alias_stored_setting")
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

    instance.query("DROP USER IF EXISTS user_with_alias_stored_setting")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_alias_stored_setting")


def test_merge_tree_setting_is_the_same_setting_under_either_name(start_cluster):
    # A `merge_tree_`-prefixed setting is carried through `Settings` as a custom setting, so its value is
    # stored under the spelling that wrote it and without its declared type. Writing the value it already
    # has changes nothing, whichever of its names is used, and writing a different value is still a change
    assert "0" == get_current_tier_value(instance)
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    instance.query("DROP USER IF EXISTS user_with_const_aliased_setting")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_const_aliased_setting")
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

    instance.query("DROP USER IF EXISTS user_with_const_aliased_setting")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_const_aliased_setting")


def test_both_names_of_a_merge_tree_setting_hold_one_value(start_cluster):
    # A value is stored under the canonical name of the setting, so the two names of one setting cannot
    # end up holding two values. A profile stating both is the same as stating the last one twice.
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    instance.query("DROP USER IF EXISTS user_with_both_names")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_both_names")
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

    instance.query("DROP USER IF EXISTS user_with_both_names")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_both_names")


def test_dropping_a_merge_tree_setting_from_a_profile_under_either_name(start_cluster):
    # A profile holds one element for one setting, so dropping it works under the name that wrote it
    # and under its other name alike
    canonical = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING_CANONICAL
    alias = MERGE_TREE_SETTINGS_PREFIX + MERGE_TREE_ALIASED_SETTING

    for stated, dropped in [(alias, canonical), (canonical, alias)]:
        instance.query("DROP SETTINGS PROFILE IF EXISTS profile_dropping_either_name")
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

    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_dropping_either_name")


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
    output, error = strict_replica.query_and_get_answer_with_error(
        f"ALTER TABLE {table} MODIFY SETTING {MERGE_TREE_EXPERIMENTAL_SETTING} = 0"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

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
