import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", user_configs=["configs/empty_settings.xml"])
node_env = cluster.add_instance(
    "node_env",
    main_configs=["configs/env_masking_rule.xml"],
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


def test_reload_keeps_system_env_reference():
    # The rule reads its pattern through `${system.env.MASK_PATTERN}`; masking must hold before and after a reload.
    node_env.query("SELECT 'env_secret_value'")
    assert_logs_contain_with_retry(node_env, r"SELECT '\[env-hidden\]'")
    assert not node_env.contains_in_log("SELECT 'env_secret_value'")
    node_env.rotate_logs()

    node_env.query("SYSTEM RELOAD CONFIG")

    node_env.query("SELECT 'env_secret_value'")
    assert_logs_contain_with_retry(node_env, r"SELECT '\[env-hidden\]'")
    assert not node_env.contains_in_log("SELECT 'env_secret_value'")
