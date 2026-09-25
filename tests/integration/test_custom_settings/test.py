import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/param_prefix.xml"])


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_custom_settings():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/custom_settings.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('custom_a')") == "-5\n"
    assert node.query("SELECT getSetting('custom_b')") == "10000000000\n"
    assert node.query("SELECT getSetting('custom_c')") == "-4.325\n"
    assert node.query("SELECT getSetting('custom_d')") == "some text\n"


def test_illformed_setting():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/illformed_setting.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    error_message = "Couldn't restore Field from dump: 1"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")


def test_param_prefixed_custom_setting_is_no_storage_setting():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/param_prefixed_setting.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")
    # Without this the rejection below would also hold for a server that knows no such custom setting.
    assert node.query("SELECT getSetting('param_x')") == "1\n"

    node.query("DROP TABLE IF EXISTS t_param SYNC")
    assert "Unknown setting 'param_x'" in node.query_and_get_error(
        "CREATE TABLE t_param (a Int) ENGINE = MergeTree ORDER BY a SETTINGS param_x = 1"
    )
    assert "Unknown setting 'param_x'" in node.query_and_get_error(
        "CREATE TABLE t_param (a Int) ENGINE = File(CSV) SETTINGS param_x = 1"
    )

    # An ordinary query setting in the same position is still accepted, and is applied rather than stored.
    node.query(
        "CREATE TABLE t_param (a Int) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096, max_threads = 1"
    )
    assert "max_threads" not in node.query("SHOW CREATE TABLE t_param")
    node.query("DROP TABLE t_param SYNC")
