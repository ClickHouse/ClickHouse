import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir="configs")




@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_max_constraint(started_cluster):
    # Change a setting for session with SET.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SET max_memory_usage=20000000000;\n"
                          "SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "20000000000\n"

    expected_error = "Setting max_memory_usage shouldn't be greater than 20000000000"
    assert expected_error in instance.query_and_get_error("SET max_memory_usage=20000000001")

    assert instance.query("SET max_memory_usage=11000000000;\n"
                          "SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "11000000000\n"

    # Change a setting for query with SETTINGS.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage' "
                          "SETTINGS max_memory_usage=20000000000") == "20000000000\n"

    assert expected_error in instance.query_and_get_error(
           "SELECT value FROM system.settings WHERE name='max_memory_usage' "
           "SETTINGS max_memory_usage=20000000001")

    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage' "
                          "SETTINGS max_memory_usage=12000000000") == "12000000000\n"
