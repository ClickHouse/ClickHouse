import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/allow_feature_tier.xml"],
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
    # We use these settings as an example. If it fails in the future because you've changed the tier of the setting
    # please change it to another setting in the same tier. If there is none, feel free to comment out the test for that tier
    query_with_experimental_setting = (
        "SELECT 1 SETTINGS allow_experimental_time_series_table=1"
    )
    query_with_beta_setting = "SELECT 1 SETTINGS allow_experimental_lightweight_update=1"

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
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert error == ""
    assert "1" == output.strip()

    # Disable experimental and beta settings
    instance.replace_in_config(feature_tier_path, "1", "2")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_setting
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output, error = instance.query_and_get_answer_with_error(query_with_beta_setting)
    assert output == ""
    assert "Changes to BETA settings are disabled" in error

    # Leave the server as it was
    instance.replace_in_config(feature_tier_path, "2", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)


def test_allow_feature_tier_in_mergetree_settings(start_cluster):
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP TABLE IF EXISTS test_experimental")

    # Disable experimental settings
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    query_with_experimental_mergetree_setting = """
        CREATE TABLE test_experimental (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS allow_experimental_replacing_merge_with_cleanup=1;
    """

    output, error = instance.query_and_get_answer_with_error(
        query_with_experimental_mergetree_setting
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

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
    assert "allow_experimental_replacing_merge_with_cleanup" in output

    # We now disable experimental settings and restart the server to confirm it boots correctly
    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    instance.restart_clickhouse()

    # After the reboot the table will be there
    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_experimental'"
    )
    assert "allow_experimental_replacing_merge_with_cleanup" in output

    # Creating a different table should not be possible
    output, error = instance.query_and_get_answer_with_error(
        """
        CREATE TABLE test_experimental_new (uid String, version UInt32, is_deleted UInt8)
        ENGINE = ReplacingMergeTree(version, is_deleted)
        ORDER by (uid)
        SETTINGS allow_experimental_replacing_merge_with_cleanup=1;
    """
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

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
        """
        ALTER TABLE test_experimental_new MODIFY setting allow_experimental_replacing_merge_with_cleanup=1
    """
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error
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
        "CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS allow_experimental_time_series_table = 1"
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    # Go back to normal and create the user to restart the server and verify it works
    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER user_experimental IDENTIFIED WITH no_password SETTINGS allow_experimental_time_series_table = 1"
    )
    assert output == ""
    assert error == ""

    # Default user = 0
    output, error = instance.query_and_get_answer_with_error(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'"
    )
    assert output.strip() == "0"
    assert error == ""

    # New user = 1
    output, error = instance.query_and_get_answer_with_error(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
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
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'"
    )
    assert output.strip() == "0"
    assert error == ""

    # New user = 1
    output, error = instance.query_and_get_answer_with_error(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""

    # But note that they can't change the value either
    # 1 - 1 => OK
    output, error = instance.query_and_get_answer_with_error(
        "SELECT 1 SETTINGS allow_experimental_time_series_table=1",
        user="user_experimental",
    )
    assert output.strip() == "1"
    assert error == ""
    # 1 - 0 => KO
    output, error = instance.query_and_get_answer_with_error(
        "SELECT 1 SETTINGS allow_experimental_time_series_table=0",
        user="user_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

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
        "allow_experimental_time_series_table>.",
        "allow_experimental_time_series_table>1",
    )

    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)
    output, error = instance.query_and_get_answer_with_error(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'"
    )
    assert output.strip() == "1"
    assert error == ""

    # But it won't be possible to change it
    output, error = instance.query_and_get_answer_with_error(
        "SELECT 1 SETTINGS allow_experimental_time_series_table=0"
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    instance.replace_in_config(feature_tier_path, "2", "0")
    instance.replace_in_config(
        "/etc/clickhouse-server/users.d/users.xml",
        "allow_experimental_time_series_table>.",
        "allow_experimental_time_series_table>0",
    )

    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)


# `MergeTree` settings are referenced in profiles with the `merge_tree_` prefix.
MERGE_TREE_PRODUCTION_SETTING = "merge_tree_max_avg_part_size_for_too_many_parts"
MERGE_TREE_EXPERIMENTAL_SETTING = "merge_tree_allow_experimental_replacing_merge_with_cleanup"


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
            f"{MERGE_TREE_PRODUCTION_SETTING} MIN 536870912 MAX 2147483648"
        )
        assert output == ""
        assert error == ""

        element = get_profile_element(
            instance, "profile_with_merge_tree_element", MERGE_TREE_PRODUCTION_SETTING
        )
        assert "536870912" in element
        assert "2147483648" in element

        # A value is allowed too, both for a user and in a query
        output, error = instance.query_and_get_answer_with_error(
            "CREATE USER user_with_merge_tree_element IDENTIFIED WITH no_password "
            f"SETTINGS {MERGE_TREE_PRODUCTION_SETTING} = 1073741824"
        )
        assert output == ""
        assert error == ""

        output, error = instance.query_and_get_answer_with_error(
            f"SELECT 1 SETTINGS {MERGE_TREE_PRODUCTION_SETTING} = 1073741824"
        )
        assert output.strip() == "1"
        assert error == ""

        # EXPERIMENTAL is rejected because of its tier, not because the name is unknown
        output, error = instance.query_and_get_answer_with_error(
            "CREATE SETTINGS PROFILE profile_with_experimental_merge_tree_element SETTINGS "
            f"{MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
        )
        assert output == ""
        assert "Changes to EXPERIMENTAL settings are disabled" in error

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

    element = get_profile_element(node, "default", MERGE_TREE_PRODUCTION_SETTING)
    assert "536870912" in element
    assert "2147483648" in element

    # And the constraint is enforced
    node.query("DROP TABLE IF EXISTS test_merge_tree_constraint")
    output, error = node.query_and_get_answer_with_error(
        "CREATE TABLE test_merge_tree_constraint (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS max_avg_part_size_for_too_many_parts = 1"
    )
    assert output == ""
    assert "shouldn't be less than 536870912" in error
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
        "SELECT value FROM system.merge_tree_settings WHERE name = 'allow_commit_order_projection'"
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
            f"SELECT 1 SETTINGS compatibility = '24.10', {MERGE_TREE_PRODUCTION_SETTING} = 1073741824"
        )
        assert output.strip() == "1"
        assert error == ""

        # Re-declaring the value already in effect (forced by the server) from a query is a no-op, exactly
        # like it is for a plain session/query setting, so it is allowed
        output, error = node.query_and_get_answer_with_error(
            "ALTER TABLE test_server_level_settings MODIFY SETTING allow_commit_order_projection = 1"
        )
        assert output == ""
        assert error == ""

        # But reverting it to the compiled default is a real change and is rejected, even though the
        # resulting value matches the compiled default
        output, error = node.query_and_get_answer_with_error(
            "ALTER TABLE test_server_level_settings MODIFY SETTING allow_commit_order_projection = 0"
        )
        assert output == ""
        assert "Changes to EXPERIMENTAL settings are disabled" in error

        # Same for `CREATE TABLE`
        output, error = node.query_and_get_answer_with_error(
            "CREATE TABLE test_experimental_revert (a UInt64) ENGINE = MergeTree ORDER BY a "
            "SETTINGS allow_commit_order_projection = 0"
        )
        assert output == ""
        assert "Changes to EXPERIMENTAL settings are disabled" in error

        # And so is any other EXPERIMENTAL setting
        output, error = node.query_and_get_answer_with_error(
            "CREATE TABLE test_experimental_server_level (a UInt64) ENGINE = MergeTree ORDER BY a "
            "SETTINGS allow_experimental_replacing_merge_with_cleanup = 1"
        )
        assert output == ""
        assert "Changes to EXPERIMENTAL settings are disabled" in error

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
        "SETTINGS allow_commit_order_projection = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER TABLE test_unrelated_alter MODIFY SETTING max_avg_part_size_for_too_many_parts = 999999999"
    )
    assert output == ""
    assert error == ""

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_unrelated_alter'"
    )
    assert "allow_commit_order_projection" in output
    assert "max_avg_part_size_for_too_many_parts" in output

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
        "SETTINGS allow_commit_order_projection = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER TABLE test_reset_bypass MODIFY SETTING allow_commit_order_projection = 0"
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output, error = instance.query_and_get_answer_with_error(
        "ALTER TABLE test_reset_bypass RESET SETTING allow_commit_order_projection"
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT engine_full FROM system.tables WHERE name = 'test_reset_bypass'"
    )
    assert "allow_commit_order_projection" in output

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
        "SETTINGS allow_experimental_time_series_table = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_bypass IDENTIFIED WITH no_password "
        "SETTINGS allow_experimental_time_series_table = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_drop_bypass SETTINGS allow_experimental_time_series_table = 0",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_drop_bypass DROP SETTING allow_experimental_time_series_table",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
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
        "SETTINGS allow_experimental_time_series_table = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_all_bypass IDENTIFIED WITH no_password "
        "SETTINGS allow_experimental_time_series_table = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_drop_all_bypass DROP ALL SETTINGS",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
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
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_drop_mt_bypass IDENTIFIED WITH no_password "
        f"SETTINGS {MERGE_TREE_EXPERIMENTAL_SETTING} = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        f"ALTER USER user_drop_mt_bypass DROP SETTING {MERGE_TREE_EXPERIMENTAL_SETTING}",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query("SHOW CREATE USER user_drop_mt_bypass")
    assert MERGE_TREE_EXPERIMENTAL_SETTING in output

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
        "SETTINGS allow_experimental_block_number_column = 1, enable_block_offset_column = 1"
    )

    output, error = instance.query_and_get_answer_with_error(
        "ALTER TABLE test_alias_preserved MODIFY SETTING max_avg_part_size_for_too_many_parts = 999999999"
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
        "SETTINGS allow_experimental_time_series_table = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE USER user_or_replace_bypass IDENTIFIED WITH no_password "
        "SETTINGS allow_experimental_time_series_table = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER OR REPLACE user_or_replace_bypass IDENTIFIED WITH no_password SETTINGS max_memory_usage = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
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
        "SETTINGS allow_experimental_time_series_table = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE SETTINGS PROFILE profile_constraint_only_bypass SETTINGS "
        "allow_experimental_time_series_table MIN 0 MAX 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER SETTINGS PROFILE profile_constraint_only_bypass DROP ALL SETTINGS",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query("SHOW CREATE SETTINGS PROFILE profile_constraint_only_bypass")
    assert "allow_experimental_time_series_table" in output

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
        "SETTINGS allow_experimental_time_series_table = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_drop_bypass_ordinary_admin DROP SETTING allow_experimental_time_series_table",
        user="ordinary_admin",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
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
        "SETTINGS allow_experimental_time_series_table = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_experimental")
    instance.query(
        "CREATE SETTINGS PROFILE profile_with_experimental_inherited SETTINGS "
        "allow_experimental_time_series_table = 1"
    )
    instance.query(
        "CREATE USER user_profile_bypass IDENTIFIED WITH no_password "
        "SETTINGS PROFILE 'profile_with_experimental_inherited'"
    )

    instance.replace_in_config(feature_tier_path, "0", "1")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "1" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "CREATE USER OR REPLACE user_profile_bypass IDENTIFIED WITH no_password SETTINGS max_memory_usage = 1",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_profile_bypass DROP PROFILES profile_with_experimental_inherited",
        user="admin_with_experimental",
    )
    assert output == ""
    assert "Changes to EXPERIMENTAL settings are disabled" in error

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_time_series_table'",
        user="user_profile_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "1", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_experimental, user_profile_bypass")
    instance.query("DROP SETTINGS PROFILE IF EXISTS profile_with_experimental_inherited")


def test_alias_mismatch_does_not_trigger_bogus_revert(start_cluster):
    # findRevertedSettingNames must compare resolved (canonical) names: a setting kept across an ALTER
    # but respelled through its alias must not be mistaken for a revert and blocked
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_beta, user_alias_no_bypass")
    instance.query(
        "CREATE USER admin_with_beta IDENTIFIED WITH no_password "
        "SETTINGS allow_experimental_delta_lake_writes = 1"
    )
    instance.query("GRANT ACCESS MANAGEMENT ON *.* TO admin_with_beta")
    instance.query(
        "CREATE USER user_alias_no_bypass IDENTIFIED WITH no_password "
        "SETTINGS allow_delta_lake_writes = 1"
    )

    instance.replace_in_config(feature_tier_path, "0", "2")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "2" == get_current_tier_value(instance)

    output, error = instance.query_and_get_answer_with_error(
        "ALTER USER user_alias_no_bypass SETTINGS allow_experimental_delta_lake_writes = 1",
        user="admin_with_beta",
    )
    assert error == ""

    output = instance.query(
        "SELECT value FROM system.settings WHERE name = 'allow_experimental_delta_lake_writes'",
        user="user_alias_no_bypass",
    )
    assert output.strip() == "1"

    instance.replace_in_config(feature_tier_path, "2", "0")
    instance.query("SYSTEM RELOAD CONFIG")
    assert "0" == get_current_tier_value(instance)
    instance.query("DROP USER IF EXISTS admin_with_beta, user_alias_no_bypass")
