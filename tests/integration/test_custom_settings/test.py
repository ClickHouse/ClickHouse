import pytest
import os
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_custom_settings():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/custom_settings.xml"), '/etc/clickhouse-server/users.d/z.xml')
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('custom_a')") == "-5\n"
    assert node.query("SELECT getSetting('custom_b')") == "10000000000\n"
    assert node.query("SELECT getSetting('custom_c')") == "-4.325\n"
    assert node.query("SELECT getSetting('custom_d')") == "some text\n"


def test_illformed_setting():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/illformed_setting.xml"), '/etc/clickhouse-server/users.d/z.xml')
    error_message = "Couldn't restore Field from dump: 1"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")
