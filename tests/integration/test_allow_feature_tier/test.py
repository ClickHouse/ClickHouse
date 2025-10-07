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

feature_tier_path = "/etc/clickhouse-server/config.d/allow_feature_tier.xml"


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
    query_with_beta_setting = "SELECT 1 SETTINGS enable_parallel_replicas=1"

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
    assert error is ""

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
