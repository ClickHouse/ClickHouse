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

feature_tier_path = "/etc/clickhouse-server/config.d/allow_feature_tier.xml"
feature_tier_1_path = "/etc/clickhouse-server/config.d/allow_feature_tier_1.xml"

# These settings are used as examples of their tier. If one changes tier in the future, please replace
# it with another setting of the same tier. If there is none, feel free to comment out the affected test.
EXPERIMENTAL_SETTING = "allow_experimental_time_series_table"  # also in configs/users.d/users.xml
BETA_SETTING = "allow_experimental_lightweight_update"
BETA_SETTING_CANONICAL = "allow_delta_lake_writes"
BETA_SETTING_ALIAS = "allow_experimental_delta_lake_writes"
PRODUCTION_SETTING = "max_memory_usage"

# A `MergeTree` setting is written by its bare name in a table's own `SETTINGS` or `ALTER ... MODIFY
# SETTING`, and with a `merge_tree_` prefix in a profile, user or session `SETTINGS` clause.
MERGE_TREE_SETTINGS_PREFIX = "merge_tree_"
MERGE_TREE_PRODUCTION_SETTING = "max_avg_part_size_for_too_many_parts"
MERGE_TREE_EXPERIMENTAL_SETTING = "allow_experimental_replacing_merge_with_cleanup"
# Set by configs/merge_tree_experimental_setting.xml
MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG = "allow_commit_order_projection"
MERGE_TREE_ALIASED_SETTING_CANONICAL = "enable_block_number_column"
MERGE_TREE_ALIASED_SETTING = "allow_experimental_block_number_column"

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
    output, error = instance.query_and_get_answer_with_error(
        f"""
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING}=1;
    """
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    # Creating a different table and altering its settings to enable experimental should not be possible either
    output, error = instance.query_and_get_answer_with_error(
        """
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid);
    """
    )
    assert output == ""
    assert error == ""

    output, error = instance.query_and_get_answer_with_error(
        f"""
        ALTER TABLE test_experimental_new MODIFY setting {MERGE_TREE_EXPERIMENTAL_SETTING}=1
    """
    )
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
        instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_merge_tree_element")
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
            instance, "profile_with_merge_tree_element", MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE
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
        assert "1" == instance.query(
            "SELECT count() FROM system.settings_profiles WHERE name = 'profile_with_merge_tree_element'"
        ).strip()

        drop_objects()
        instance.replace_in_config(feature_tier_path, tier, "0")
        instance.query("SYSTEM RELOAD CONFIG")
        assert "0" == get_current_tier_value(instance)


def test_merge_tree_constraint_in_config_with_feature_tier(start_cluster):
    # The server must start with such a constraint in the config and the feature tier not 0
    node = instance_with_merge_tree_constraint
    assert "1" == get_current_tier_value(node)
    assert "1" == node.query("SELECT 1").strip()

    element = get_profile_element(node, "default", MERGE_TREE_PRODUCTION_SETTING_IN_PROFILE)
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


def test_server_level_merge_tree_settings_are_not_blocked_by_feature_tier(start_cluster):
    # `compatibility` and the `merge_tree` config section can change the value of EXPERIMENTAL/BETA settings.
    # They are set by the server, not by a query, so they must not make every table creation fail
    node = instance_with_merge_tree_constraint
    assert "24.10" == node.query(
        "SELECT value FROM system.settings WHERE name = 'compatibility'"
    ).strip()
    assert "1" == node.query(
        f"SELECT value FROM system.merge_tree_settings WHERE name = '{MERGE_TREE_EXPERIMENTAL_SETTING_IN_CONFIG}'"
    ).strip()

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


def test_drop_setting_bypassing_feature_tier_for_user(start_cluster):
    # DROP SETTING must be checked like MODIFY SETTING to the default value. The check compares against
    # the acting user's own session, so that user needs the setting itself (granted while the tier was
    # permissive) for the comparison to actually engage
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_bypass")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_drop_bypass SETTINGS {EXPERIMENTAL_SETTING} = 0",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_drop_bypass DROP SETTING {EXPERIMENTAL_SETTING}",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_drop_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_bypass")


def test_drop_all_settings_bypassing_feature_tier(start_cluster):
    # DROP ALL SETTINGS must be checked the same way as DROP SETTING for every setting it removes
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_all_bypass")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_all_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_drop_all_bypass DROP ALL SETTINGS",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_drop_all_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_all_bypass")


def test_drop_setting_bypassing_feature_tier_for_merge_tree_prefixed_setting(
    start_cluster,
):
    # A `merge_tree_`-prefixed setting dropped from a user/role/profile must be checked too: it is a
    # MergeTreeSettings name carried through `Settings` as a custom setting, not a `Settings` builtin
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_mt_bypass")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_mt_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_drop_mt_bypass DROP SETTING {MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE}",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query("SHOW CREATE USER user_drop_mt_bypass")
    assert MERGE_TREE_EXPERIMENTAL_SETTING_IN_PROFILE in output

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_drop_mt_bypass")


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


def test_create_or_replace_user_bypassing_feature_tier(start_cluster):
    # `CREATE USER OR REPLACE` is a full replacement too (like an old-style `ALTER ... SETTINGS`), so it
    # must be checked the same way when it drops a previously granted EXPERIMENTAL/BETA override
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_or_replace_bypass")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_or_replace_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER OR REPLACE user_or_replace_bypass IDENTIFIED WITH no_password SETTINGS {PRODUCTION_SETTING} = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_or_replace_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_or_replace_bypass")


def test_drop_all_settings_bypassing_feature_tier_for_constraint_only_element(
    start_cluster,
):
    # A profile element that only carries MIN/MAX (no plain value) must be checked too when dropped:
    # SettingsConstraints::check() gates min_value/max_value against the tier just like value
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_constraint_only_bypass")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE SETTINGS PROFILE profile_constraint_only_bypass SETTINGS "
        f"{EXPERIMENTAL_SETTING} MIN 0 MAX 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_constraint_only_bypass DROP ALL SETTINGS",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query("SHOW CREATE SETTINGS PROFILE profile_constraint_only_bypass")
    assert EXPERIMENTAL_SETTING in output

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_constraint_only_bypass")


def test_drop_setting_bypassing_feature_tier_for_ordinary_admin(start_cluster):
    # DROP SETTING must be checked against the target's own value, not the acting admin's session: an
    # ordinary admin who never touched the setting (so it reads as its compiled default in their own
    # session) must not be able to strip a legitimately-granted EXPERIMENTAL override from someone else
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS ordinary_admin, user_drop_bypass_ordinary_admin")
    instance.query("CREATE USER ordinary_admin IDENTIFIED WITH no_password")
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO ordinary_admin")
    instance.query(
        "CREATE USER user_drop_bypass_ordinary_admin IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_drop_bypass_ordinary_admin DROP SETTING {EXPERIMENTAL_SETTING}",
        user="ordinary_admin",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_drop_bypass_ordinary_admin",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS ordinary_admin, user_drop_bypass_ordinary_admin")


def test_replacing_inherited_profile_bypassing_feature_tier(start_cluster):
    # A setting inherited only through an attached profile must be checked too: dropping or replacing
    # that profile removes the effective EXPERIMENTAL/BETA value just as much as dropping it directly
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_profile_bypass")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_experimental_inherited")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE SETTINGS PROFILE profile_with_experimental_inherited SETTINGS "
        f"{EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(
        "CREATE USER user_profile_bypass IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_experimental_inherited'"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"CREATE USER OR REPLACE user_profile_bypass IDENTIFIED WITH no_password SETTINGS {PRODUCTION_SETTING} = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_profile_bypass DROP PROFILES profile_with_experimental_inherited",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_profile_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_profile_bypass")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_experimental_inherited")


def test_alias_mismatch_does_not_trigger_bogus_revert(start_cluster):
    # findChangedSettings must compare resolved (canonical) names: a setting kept across an ALTER
    # but respelled through its alias must not be mistaken for a revert and blocked
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_beta, user_alias_no_bypass")
    instance.query(
        "CREATE USER admin_with_beta IDENTIFIED WITH no_password "
        f"SETTINGS {BETA_SETTING_ALIAS} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_beta")
    instance.query(
        "CREATE USER user_alias_no_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {BETA_SETTING_CANONICAL} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "2")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_alias_no_bypass SETTINGS {BETA_SETTING_ALIAS} = 1",
        user="admin_with_beta",
    )
    assert error == ""

    output = instance.query(
        f"SELECT value FROM system.settings WHERE name = '{BETA_SETTING_ALIAS}'",
        user="user_alias_no_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "2", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_beta, user_alias_no_bypass")


def test_replacing_profile_that_shadows_a_direct_override(start_cluster):
    # The effective value of a setting is the last one that wins after the attached profiles are
    # substituted. A change that leaves the name present somewhere but flips the value that wins is a
    # change like any other: here the direct `= 0` shadows the `= 1` inherited from the profile, so
    # removing it turns the EXPERIMENTAL setting back on and must be checked
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_shadowing_profile")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_shadowed_by_user")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE SETTINGS PROFILE profile_shadowed_by_user SETTINGS "
        f"{EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(
        "CREATE USER user_shadowing_profile IDENTIFIED WITH no_password SETTINGS "
        f"PROFILE 'profile_shadowed_by_user', {EXPERIMENTAL_SETTING} = 0"
    )
    assert "0" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_shadowing_profile",
    ).strip()

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER OR REPLACE user_shadowing_profile IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_shadowed_by_user'",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_shadowing_profile DROP SETTING {EXPERIMENTAL_SETTING}",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "0" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_shadowing_profile",
    ).strip()

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_shadowing_profile")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_shadowed_by_user")


def test_granting_a_setting_the_admin_already_has_is_checked(start_cluster):
    # The value granted to another entity must be compared with what that entity has now, not with the
    # acting admin's own session: an admin who was granted the EXPERIMENTAL setting while the tier was
    # permissive must not be able to pass it on afterwards just because it matches their own value
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_granted_experimental")
    instance.query(
        "CREATE USER admin_with_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query("CREATE USER user_granted_experimental IDENTIFIED WITH no_password")

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_granted_experimental SETTINGS {EXPERIMENTAL_SETTING} = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER new_user_granted_experimental IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "0" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_granted_experimental",
    ).strip()

    # A PRODUCTION setting is still grantable, and so is re-stating the value the target already has
    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_granted_experimental SETTINGS {PRODUCTION_SETTING} = 1000000000",
        user="admin_with_experimental",
    )
    assert output == ""
    assert error == ""

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query(
        "DROP USER IF EXISTS admin_with_experimental, user_granted_experimental, new_user_granted_experimental"
    )


def test_overriding_setting_of_another_user_is_checked_against_that_user(start_cluster):
    # The mirror case of the test above: an ordinary admin, whose own session reads the compiled default,
    # must not be able to turn off an EXPERIMENTAL setting someone else was legitimately granted, neither
    # by writing the default explicitly nor by omitting it from a full replacement
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS ordinary_admin, user_with_experimental_setting")
    instance.query("CREATE USER ordinary_admin IDENTIFIED WITH no_password")
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO ordinary_admin")
    instance.query(
        "CREATE USER user_with_experimental_setting IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1, {PRODUCTION_SETTING} = 1000000000"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_with_experimental_setting SETTINGS {EXPERIMENTAL_SETTING} = 0",
        user="ordinary_admin",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_with_experimental_setting SETTINGS {PRODUCTION_SETTING} = 2000000000",
        user="ordinary_admin",
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "1" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_with_experimental_setting",
    ).strip()

    # Changing only the PRODUCTION setting, and leaving the experimental one where it is, is allowed
    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_with_experimental_setting MODIFY SETTING {PRODUCTION_SETTING} = 2000000000",
        user="ordinary_admin",
    )
    assert output == ""
    assert error == ""

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS ordinary_admin, user_with_experimental_setting")


def test_granting_role_carrying_an_experimental_setting(start_cluster):
    # A role carries settings without naming them in the GRANT, so granting it hands the target an
    # EXPERIMENTAL setting just as much as an explicit SETTINGS clause would, and revoking it takes it away
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_granted_role, user_revoked_role")
    instance.query("DROP ROLE IF EXISTS role_with_experimental")
    instance.query("CREATE USER user_granted_role IDENTIFIED WITH no_password")
    instance.query("CREATE USER user_revoked_role IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE ROLE role_with_experimental SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT role_with_experimental TO user_revoked_role")

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "GRANT role_with_experimental TO user_granted_role"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        "REVOKE role_with_experimental FROM user_revoked_role"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    # A role that carries nothing is unaffected
    instance.query("DROP ROLE IF EXISTS role_without_settings")
    instance.query("CREATE ROLE role_without_settings")
    output, error = instance.query_and_get_answer_with_error(
        "GRANT role_without_settings TO user_granted_role"
    )
    assert output == ""
    assert error == ""

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_granted_role, user_revoked_role")
    instance.query("DROP ROLE IF EXISTS role_with_experimental, role_without_settings")


def test_assigning_profile_carrying_an_experimental_setting(start_cluster):
    # Same for the `TO` clause of a settings profile: it names no setting, but it decides who the
    # EXPERIMENTAL setting the profile carries applies to
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_assigned_profile")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_assigned_to_user")
    instance.query("CREATE USER user_assigned_profile IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE SETTINGS PROFILE profile_assigned_to_user SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_assigned_to_user TO user_assigned_profile"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "0" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_assigned_profile",
    ).strip()

    # A profile carrying only a PRODUCTION setting can still be assigned
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_production_only")
    instance.query(
        f"CREATE SETTINGS PROFILE profile_production_only SETTINGS {PRODUCTION_SETTING} = 1000000000"
    )
    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_production_only TO user_assigned_profile"
    )
    assert output == ""
    assert error == ""

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_assigned_profile")
    instance.query(
        "DROP SETTINGS PROFILE IF EXISTS profile_assigned_to_user, profile_production_only"
    )


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


def test_dropping_a_const_constraint_on_an_experimental_setting(start_cluster):
    # A constraint-only element carries no value, but making an EXPERIMENTAL setting CONST or letting it
    # become writable again is still a change to that setting
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_const_only")
    instance.query(
        f"CREATE SETTINGS PROFILE profile_const_only SETTINGS {EXPERIMENTAL_SETTING} CONST"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_const_only DROP ALL SETTINGS"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output = instance.query("SHOW CREATE SETTINGS PROFILE profile_const_only")
    assert EXPERIMENTAL_SETTING in output

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_const_only")


def test_create_or_replace_without_a_settings_clause(start_cluster):
    # A replacement states the whole entity, so leaving the settings clause out replaces the settings with
    # nothing: the EXPERIMENTAL override disappears exactly as it would with an explicit clause
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_replaced_without_clause")
    instance.query("DROP ROLE IF EXISTS role_replaced_without_clause")
    instance.query(
        "CREATE USER user_replaced_without_clause IDENTIFIED WITH no_password "
        f"SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query(
        f"CREATE ROLE role_replaced_without_clause SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER OR REPLACE user_replaced_without_clause IDENTIFIED WITH no_password"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        "CREATE ROLE OR REPLACE role_replaced_without_clause"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "1" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_replaced_without_clause",
    ).strip()

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_replaced_without_clause")
    instance.query("DROP ROLE IF EXISTS role_replaced_without_clause")


def test_granting_role_that_carries_another_role(start_cluster):
    # A granted role brings the roles granted to it along, so the EXPERIMENTAL setting can sit one level
    # down and still become effective
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_granted_parent_role")
    instance.query("DROP ROLE IF EXISTS parent_role, child_role_with_experimental")
    instance.query("CREATE USER user_granted_parent_role IDENTIFIED WITH no_password")
    instance.query(
        f"CREATE ROLE child_role_with_experimental SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE ROLE parent_role")
    instance.query("GRANT child_role_with_experimental TO parent_role")

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "GRANT parent_role TO user_granted_parent_role"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_granted_parent_role")
    instance.query("DROP ROLE IF EXISTS parent_role, child_role_with_experimental")


def test_making_a_granted_role_default(start_cluster):
    # A role that is granted but not default carries nothing yet. Making it default, or making it stop
    # being default, decides whether its EXPERIMENTAL setting applies
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_with_non_default_role")
    instance.query("DROP ROLE IF EXISTS non_default_role_with_experimental")
    instance.query(
        f"CREATE ROLE non_default_role_with_experimental SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE USER user_with_non_default_role IDENTIFIED WITH no_password")
    instance.query(
        "GRANT non_default_role_with_experimental TO user_with_non_default_role"
    )
    instance.query("SET DEFAULT ROLE NONE TO user_with_non_default_role")

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "SET DEFAULT ROLE non_default_role_with_experimental TO user_with_non_default_role"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_with_non_default_role DEFAULT ROLE non_default_role_with_experimental"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "0" == instance.query(
        f"SELECT value FROM system.settings WHERE name = '{EXPERIMENTAL_SETTING}'",
        user="user_with_non_default_role",
    ).strip()

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_with_non_default_role")
    instance.query("DROP ROLE IF EXISTS non_default_role_with_experimental")


def test_creating_a_user_with_a_role_clause(start_cluster):
    # `CREATE USER ... ROLE r` grants the role right there instead of through `GRANT`, so it hands the new
    # user everything that role carries
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_created_with_role")
    instance.query("DROP ROLE IF EXISTS role_clause_with_experimental")
    instance.query(
        f"CREATE ROLE role_clause_with_experimental SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER user_created_with_role IDENTIFIED WITH no_password "
        "DEFAULT ROLE role_clause_with_experimental ROLE role_clause_with_experimental"
    )
    assert output == ""
    assert EXPERIMENTAL_BLOCKED in error

    assert "0" == instance.query(
        "SELECT count() FROM system.users WHERE name = 'user_created_with_role'"
    ).strip()

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_created_with_role")
    instance.query("DROP ROLE IF EXISTS role_clause_with_experimental")


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


def test_redundant_grant_and_assignment_are_not_rejected(start_cluster):
    # Only what really starts or stops being granted counts. Re-granting a role the user already has,
    # granting the admin option for it, or restating the same profile assignment changes no setting
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_already_granted")
    instance.query("DROP ROLE IF EXISTS role_already_granted")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_already_assigned")
    instance.query(
        f"CREATE ROLE role_already_granted SETTINGS {EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("CREATE USER user_already_granted IDENTIFIED WITH no_password")
    instance.query("GRANT role_already_granted TO user_already_granted")
    instance.query(
        f"CREATE SETTINGS PROFILE profile_already_assigned SETTINGS {EXPERIMENTAL_SETTING} = 1 "
        "TO user_already_granted"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "GRANT role_already_granted TO user_already_granted"
    )
    assert output == ""
    assert error == ""

    output, error = instance.query_and_get_answer_with_error(
        "GRANT role_already_granted TO user_already_granted WITH ADMIN OPTION"
    )
    assert output == ""
    assert error == ""

    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_already_assigned TO user_already_granted"
    )
    assert output == ""
    assert error == ""

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS user_already_granted")
    instance.query("DROP ROLE IF EXISTS role_already_granted")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_already_assigned")
