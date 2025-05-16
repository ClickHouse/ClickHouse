import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", clickhouse_path_dir="clickhouse_path")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query("CREATE DATABASE test")
        yield cluster

    finally:
        cluster.shutdown()


def create_simple_table():
    instance.query("DROP TABLE IF EXISTS test.simple")
    instance.query(
        """
        CREATE TABLE test.simple (key UInt64, value String)
            ENGINE = MergeTree ORDER BY tuple();
        """
    )


def test_protobuf_format_input(started_cluster):
    create_simple_table()
    instance.http_query(
        "INSERT INTO test.simple SETTINGS format_schema='simple:KeyValuePair' FORMAT Protobuf",
        "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
    )
    assert instance.query("SELECT * from test.simple") == "1\tabc\n2\tdef\n"


def test_protobuf_format_output(started_cluster):
    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")
    assert (
        instance.http_query(
            "SELECT * FROM test.simple FORMAT Protobuf SETTINGS format_schema='simple:KeyValuePair'"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )


def test_drop_cache_protobuf_format(started_cluster):
    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")

    schema = """
syntax = "proto3";

message MessageTmp {
  uint64 key = 1;
  string value = 2;
}
"""

    protobuf_schema_path_name = "message_tmp.proto"

    database_path = os.path.abspath(os.path.join(instance.path, "database"))
    with open(
        os.path.join(database_path, "format_schemas", protobuf_schema_path_name), "w"
    ) as file:
        file.write(schema)
    assert (
        instance.http_query(
            "SELECT * FROM test.simple FORMAT Protobuf SETTINGS format_schema='message_tmp:MessageTmp'"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )
    # Replace simple.proto with a new Protobuf schema
    new_schema = """
syntax = "proto3";

message MessageTmp {
  uint64 key2 = 1;
  string value2 = 2;
}
"""
    with open(
        os.path.join(database_path, "format_schemas", protobuf_schema_path_name), "w"
    ) as file:
        file.write(new_schema)

    instance.query("DROP TABLE IF EXISTS test.new_simple")
    instance.query(
        """
        CREATE TABLE test.new_simple (key2 UInt64, value2 String)
            ENGINE = MergeTree ORDER BY tuple();
        """
    )
    instance.query("INSERT INTO test.new_simple VALUES (1, 'abc'), (2, 'def')")

    instance.query("SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf")

    # Tets works with new scheme
    assert (
        instance.http_query(
            "SELECT * FROM test.new_simple FORMAT Protobuf SETTINGS format_schema='message_tmp:MessageTmp'"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )
    # Tests that stop working with old scheme
    with pytest.raises(Exception) as exc:
        instance.http_query(
            "SELECT * FROM test.simple FORMAT Protobuf SETTINGS format_schema='message_tmp:MessageTmp'"
        )
    assert "NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS)" in str(exc.value)


def test_drop_capn_proto_format(started_cluster):
    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")
    capn_proto_schema = """
@0x801f030c2b67bf19;

struct MessageTmp {
    key @0 :UInt64;
    value @1 :Text;
}
"""
    capn_schema_path_name = "message_tmp.capnp"

    database_path = os.path.abspath(os.path.join(instance.path, "database"))
    format_schemas_path = os.path.join(database_path, "format_schemas")
    with open(os.path.join(format_schemas_path, capn_schema_path_name), "w") as file:
        file.write(capn_proto_schema)

    assert instance.http_query(
        "SELECT * FROM test.simple FORMAT CapnProto SETTINGS format_schema='message_tmp:MessageTmp'"
    ) == instance.query(
        f"SELECT * FROM test.simple Format CapnProto SETTINGS format_schema='{format_schemas_path}/message_tmp:MessageTmp'"
    )

    new_schema = """
@0x801f030c2b67bf19;

struct MessageTmp {
    key2 @0 :UInt64;
    value2 @1 :Text;
}
"""
    with open(os.path.join(format_schemas_path, capn_schema_path_name), "w") as file:
        file.write(new_schema)

    instance.query("DROP TABLE IF EXISTS test.new_simple")
    instance.query(
        """
        CREATE TABLE test.new_simple (key2 UInt64, value2 String)
            ENGINE = MergeTree ORDER BY tuple();
        """
    )
    instance.query("INSERT INTO test.new_simple VALUES (1, 'abc'), (2, 'def')")

    # instance.query("SYSTEM DROP FORMAT SCHEMA CACHE FOR CapnProto")

    # Tets works with new scheme
    assert instance.http_query(
        "SELECT * FROM test.new_simple FORMAT CapnProto SETTINGS format_schema='message_tmp:MessageTmp'"
    ) == instance.query(
        f"SELECT * FROM test.new_simple Format CapnProto SETTINGS format_schema='{format_schemas_path}/message_tmp:MessageTmp'"
    )
    # Tests that stop working with old scheme
    with pytest.raises(Exception) as exc:
        instance.http_query(
            "SELECT * FROM test.simple FORMAT CapnProto SETTINGS format_schema='message_tmp:MessageTmp'"
        )
    assert (
        "Capnproto schema doesn't contain field with name key. (THERE_IS_NO_COLUMN)"
        in str(exc.value)
    )
