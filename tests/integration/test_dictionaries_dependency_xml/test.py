import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

DICTIONARY_FILES = ['configs/dictionaries/dep_x.xml', 'configs/dictionaries/dep_y.xml',
                    'configs/dictionaries/dep_z.xml']

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', dictionaries=DICTIONARY_FILES)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance.query('''
            CREATE DATABASE IF NOT EXISTS dict ENGINE=Dictionary;
            CREATE DATABASE IF NOT EXISTS test;
            DROP TABLE IF EXISTS test.elements;
            CREATE TABLE test.elements (id UInt64, a String, b Int32, c Float64) ENGINE=Log;
            INSERT INTO test.elements VALUES (0, 'water', 10, 1), (1, 'air', 40, 0.01), (2, 'earth', 100, 1.7);
            ''')

        yield cluster

    finally:
        cluster.shutdown()


def get_status(dictionary_name):
    return instance.query("SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'").rstrip("\n")


def test_get_data(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so these dictionary are not loaded.
    assert get_status('dep_x') == 'NOT_LOADED'
    assert get_status('dep_y') == 'NOT_LOADED'
    assert get_status('dep_z') == 'NOT_LOADED'

    # Dictionary 'dep_x' depends on 'dep_z', which depends on 'dep_y'.
    # So they all should be loaded at once.
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(1))") == "air\n"
    assert get_status('dep_x') == 'LOADED'
    assert get_status('dep_y') == 'LOADED'
    assert get_status('dep_z') == 'LOADED'

    # Other dictionaries should work too.
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(1))") == "air\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(1))") == "air\n"

    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "YY\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # Update the source table.
    query("INSERT INTO test.elements VALUES (3, 'fire', 30, 8)")

    # Wait for dictionaries to be reloaded.
    assert_eq_with_retry(instance, "SELECT dictHas('dep_y', toUInt64(3))", "1", sleep_time=2, retry_count=10)
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # dep_x and dep_z are updated only when there `intDiv(count(), 5)`  is changed.
    query("INSERT INTO test.elements VALUES (4, 'ether', 404, 0.001)")
    assert_eq_with_retry(instance, "SELECT dictHas('dep_x', toUInt64(4))", "1", sleep_time=2, retry_count=10)
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(4))") == "ether\n"
