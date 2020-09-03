import pytest
import os
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', user_configs=["configs/normal_settings.xml"])

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
        node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/normal_settings.xml"), '/etc/clickhouse-server/users.d/z.xml')
        node.query("SYSTEM RELOAD CONFIG")
        yield
    finally:
        pass


def test_force_reload():
    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"), '/etc/clickhouse-server/users.d/z.xml')
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "20000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "nearest_hostname\n"


def test_reload_on_timeout():
    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"

    time.sleep(1) # The modification time of the 'z.xml' file should be different,
                  # because config files are reload by timer only when the modification time is changed.
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"), '/etc/clickhouse-server/users.d/z.xml')

    assert_eq_with_retry(node, "SELECT getSetting('max_memory_usage')", "20000000000")
    assert_eq_with_retry(node, "SELECT getSetting('load_balancing')", "nearest_hostname")


def test_unknown_setting_force_reload():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/unknown_setting.xml"), '/etc/clickhouse-server/users.d/z.xml')

    error_message = "Setting xyz is neither a builtin setting nor started with the prefix 'custom_' registered for user-defined settings"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"


def test_unknown_setting_reload_on_timeout():
    time.sleep(1) # The modification time of the 'z.xml' file should be different,
                  # because config files are reload by timer only when the modification time is changed.
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/unknown_setting.xml"), '/etc/clickhouse-server/users.d/z.xml')

    error_message = "Setting xyz is neither a builtin setting nor started with the prefix 'custom_' registered for user-defined settings"
    assert_logs_contain_with_retry(node, error_message)

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"


def test_unexpected_setting_int():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/unexpected_setting_int.xml"), '/etc/clickhouse-server/users.d/z.xml')
    error_message = "Cannot parse"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"


def test_unexpected_setting_enum():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/unexpected_setting_int.xml"), '/etc/clickhouse-server/users.d/z.xml')
    error_message = "Cannot parse"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
