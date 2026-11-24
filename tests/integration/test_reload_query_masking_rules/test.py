import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", user_configs=["configs/empty_settings.xml"], stay_alive=True
)
node_properties = cluster.add_instance(
    "node_properties",
    main_configs=["configs/property_masking_rule.xml"],
    env_variables={"MASK_PATTERN": "env_secret_value"},
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_to_normal_settings_after_test():
    try:
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/empty_settings.xml"),
            "/etc/clickhouse-server/config.d/z.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")
        yield
    finally:
        pass


# @pytest.mark.parametrize("reload_strategy", ["force", "timeout"])
def test_reload_query_masking_rules():
    # At first, empty configuration is fed to ClickHouse. The query
    # "SELECT 'TOPSECRET.TOPSECRET'" will not be redacted, and the new masking
    # event will not be registered
    node.query("SELECT 'TOPSECRET.TOPSECRET'")
    assert_logs_contain_with_retry(node, "SELECT 'TOPSECRET.TOPSECRET'")
    assert not node.contains_in_log(r"SELECT '\[hidden\]'")
    node.rotate_logs()

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )

    node.query("SYSTEM RELOAD CONFIG")

    # Now the same query will be redacted in the logs and the counter of events
    # will be incremented
    node.query("SELECT 'TOPSECRET.TOPSECRET'")

    assert_logs_contain_with_retry(node, r"SELECT '\[hidden\]'")
    assert not node.contains_in_log("SELECT 'TOPSECRET.TOPSECRET'")

    node.rotate_logs()


def test_reload_keeps_property_references():
    # The rule pattern `${application.name}_${system.env.MASK_PATTERN}` resolves to `clickhouse_env_secret_value`
    # (the server is started as `clickhouse server`); masking must hold before and after a reload.
    node_properties.query("SELECT 'clickhouse_env_secret_value'")
    assert_logs_contain_with_retry(node_properties, r"SELECT '\[property-hidden\]'")
    assert not node_properties.contains_in_log("SELECT 'clickhouse_env_secret_value'")
    node_properties.rotate_logs()

    node_properties.query("SYSTEM RELOAD CONFIG")

    node_properties.query("SELECT 'clickhouse_env_secret_value'")
    assert_logs_contain_with_retry(node_properties, r"SELECT '\[property-hidden\]'")
    assert not node_properties.contains_in_log("SELECT 'clickhouse_env_secret_value'")


def test_sql_rule_survives_config_reload_and_restart():
    node.query(
        "CREATE QUERY MASKING RULE sql_rule REGEXP 'SQLSECRET_[0-9]+' REPLACE '[sql-hidden]'"
    )
    try:
        node.rotate_logs()

        # A config reload replaces only the rules from the config.
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"),
            "/etc/clickhouse-server/config.d/z.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")

        node.query("SELECT 'TOPSECRET.TOPSECRET', 'SQLSECRET_1'")
        assert_logs_contain_with_retry(node, r"SELECT '\[hidden\]', '\[sql-hidden\]'")
        assert not node.contains_in_log("SQLSECRET_1")
        node.rotate_logs()

        # The rule is read back from the access storage at startup.
        node.restart_clickhouse()
        node.query("SELECT 'SQLSECRET_2'")
        assert_logs_contain_with_retry(node, r"SELECT '\[sql-hidden\]'")
        assert not node.contains_in_log("SQLSECRET_2")
    finally:
        node.query("DROP QUERY MASKING RULE IF EXISTS sql_rule")


def test_sql_rule_applies_when_its_create_has_little_memory():
    # Makes small allocations honour the memory limit of the query making them.
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/throw_on_small_allocations.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    big_rules = [f"big_rule_{i}" for i in range(24)]
    big_regexp = "[A-M]{1000}" * 20
    try:
        for name in big_rules:
            node.query(
                f"CREATE QUERY MASKING RULE {name} REGEXP '{big_regexp}' REPLACE '[big]'"
            )

        # Creating one rule fits into this limit, compiling all the rules does not.
        node.query(
            "CREATE QUERY MASKING RULE limited_rule REGEXP 'LIMITSECRET_[0-9]+' REPLACE '[limit-hidden]'",
            settings={"max_memory_usage": 3000000, "max_untracked_memory": 0},
        )

        node.rotate_logs()
        node.query("SELECT 'LIMITSECRET_1'")
        assert_logs_contain_with_retry(node, r"SELECT '\[limit-hidden\]'")
        assert not node.contains_in_log("LIMITSECRET_1")
    finally:
        node.query(
            f"DROP QUERY MASKING RULE IF EXISTS limited_rule, {', '.join(big_rules)}"
        )
