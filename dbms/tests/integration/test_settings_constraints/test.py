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


def test_read_only_constraint(started_cluster):
    # Change a setting for session with SET.
    assert instance.query("SELECT value FROM system.settings WHERE name='force_index_by_date'") ==\
           "0\n"

    expected_error = "Setting force_index_by_date should not be changed"
    assert expected_error in instance.query_and_get_error("SET force_index_by_date=1")

    # Change a setting for query with SETTINGS.
    assert instance.query("SELECT value FROM system.settings WHERE name='force_index_by_date'") ==\
           "0\n"

    assert expected_error in instance.query_and_get_error(
           "SELECT value FROM system.settings WHERE name='force_index_by_date' "
           "SETTINGS force_index_by_date=1")


def test_min_constraint(started_cluster):
    # Change a setting for session with SET.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SET max_memory_usage=5000000000;\n"
                          "SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "5000000000\n"

    expected_error = "Setting max_memory_usage shouldn't be less than 5000000000"
    assert expected_error in instance.query_and_get_error("SET max_memory_usage=4999999999")

    # Change a setting for query with SETTINGS.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SET max_memory_usage=5000000001;\n"
                          "SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "5000000001\n"

    assert expected_error in instance.query_and_get_error(
           "SELECT value FROM system.settings WHERE name='max_memory_usage' "
           "SETTINGS max_memory_usage=4999999999")


def test_max_constraint(started_cluster):
    # Change a setting for session with SET.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SET max_memory_usage=20000000000;\n"
                          "SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "20000000000\n"

    expected_error = "Setting max_memory_usage shouldn't be greater than 20000000000"
    assert expected_error in instance.query_and_get_error("SET max_memory_usage=20000000001")

     # Change a setting for query with SETTINGS.
    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage'") ==\
           "10000000000\n"

    assert instance.query("SELECT value FROM system.settings WHERE name='max_memory_usage' "
                          "SETTINGS max_memory_usage=19999999999") == "19999999999\n"

    assert expected_error in instance.query_and_get_error(
           "SELECT value FROM system.settings WHERE name='max_memory_usage' "
           "SETTINGS max_memory_usage=20000000001")
 