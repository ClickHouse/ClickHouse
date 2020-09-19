import pytest
from helpers.cluster import ClickHouseCluster

ENABLE_DICT_CONFIG = ['configs/enable_dictionaries.xml']
DICTIONARY_FILES = ['configs/dictionaries/cache.xml']

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=ENABLE_DICT_CONFIG + DICTIONARY_FILES)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        instance.query('''
            CREATE DATABASE IF NOT EXISTS test;
            DROP TABLE IF EXISTS test.source;
            CREATE TABLE test.source (id UInt64, key0 UInt8, key0_str String, key1 UInt8,
                    StartDate Date, EndDate Date,
                    UInt8_ UInt8, UInt16_ UInt16, UInt32_ UInt32, UInt64_ UInt64,
                    Int8_ Int8, Int16_ Int16, Int32_ Int32, Int64_ Int64,
                    Float32_ Float32, Float64_ Float64,
                    String_ String,
                    Date_ Date, DateTime_ DateTime, Parent UInt64) ENGINE=Log;
            ''')

        yield cluster

    finally:
        cluster.shutdown()


def test_null_value(started_cluster):
    query = instance.query

    assert query("select dictGetUInt8('cache', 'UInt8_', toUInt64(12121212))") == "1\n"
    assert query("select dictGetString('cache', 'String_', toUInt64(12121212))") == "implicit-default\n"
    assert query("select dictGetDate('cache', 'Date_', toUInt64(12121212))") == "2015-11-25\n"

    # Check, that empty null_value interprets as default value
    assert query("select dictGetUInt64('cache', 'UInt64_', toUInt64(12121212))") == "0\n"
    assert query(
        "select toTimeZone(dictGetDateTime('cache', 'DateTime_', toUInt64(12121212)), 'UTC')") == "1970-01-01 00:00:00\n"
