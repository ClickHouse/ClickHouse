import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                clickhouse_path_dir='clickhouse_path')


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')
        yield cluster

    finally:
        cluster.shutdown()


def create_simple_table():
    instance.query("DROP TABLE IF EXISTS test.simple")
    instance.query('''
        CREATE TABLE test.simple (key UInt64, value String)
            ENGINE = MergeTree ORDER BY tuple();
        ''')


def test_protobuf_format_input(started_cluster):
    create_simple_table()
    instance.http_query(
        "INSERT INTO test.simple FORMAT Protobuf SETTINGS format_schema='simple:KeyValuePair'",
        "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def")
    assert instance.query("SELECT * from test.simple") == "1\tabc\n2\tdef\n"


def test_protobuf_format_output(started_cluster):
    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')");
    assert instance.http_query(
        "SELECT * FROM test.simple FORMAT Protobuf SETTINGS format_schema='simple:KeyValuePair'") == \
           "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
