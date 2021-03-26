import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir='configs')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test():
    assert node.query("SELECT getSetting('custom_a')") == "-5\n"
    assert node.query("SELECT getSetting('custom_b')") == "10000000000\n"
    assert node.query("SELECT getSetting('custom_c')") == "-4.325\n"
    assert node.query("SELECT getSetting('custom_d')") == "some text\n"

    assert "custom_a = -5, custom_b = 10000000000, custom_c = -4.325, custom_d = \\'some text\\'" \
        in node.query("SHOW CREATE SETTINGS PROFILE default")

    assert "no settings profile" in node.query_and_get_error("SHOW CREATE SETTINGS PROFILE profile_with_unknown_setting")
    assert "no settings profile" in node.query_and_get_error("SHOW CREATE SETTINGS PROFILE profile_illformed_setting")


def test_invalid_settings():
    node.query("SYSTEM RELOAD CONFIG")
    node.query("SYSTEM FLUSH LOGS")

    assert node.query("SELECT COUNT() FROM system.text_log WHERE"
        " message LIKE '%Could not parse profile `profile_illformed_setting`%'"
        " AND message LIKE '%Couldn\\'t restore Field from dump%'") == "1\n"

    assert node.query("SELECT COUNT() FROM system.text_log WHERE"
        " message LIKE '%Could not parse profile `profile_with_unknown_setting`%'"
        " AND message LIKE '%Setting x is neither a builtin setting nor started with the prefix \\'custom_\\'%'") == "1\n"
