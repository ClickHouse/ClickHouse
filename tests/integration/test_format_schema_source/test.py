import os
import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", clickhouse_path_dir="clickhouse_path")
database_path = os.path.abspath(os.path.join(instance.path, "database"))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
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


def get_format_schema(
    format_schema_source: str,
    schema_content: str,
    target_file_name: str = "",
    file_extention="proto",
):
    if format_schema_source == "file":
        if target_file_name == "":
            target_file_name = f"__generated__{uuid.uuid4().hex}.{file_extention}"
        file_path = os.path.join(database_path, "format_schemas", target_file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
        with open(file_path, "w") as file:
            file.write(schema_content)
        return target_file_name

    if format_schema_source == "string":
        return schema_content

    if format_schema_source == "query":
        instance.query(
            "CREATE OR REPLACE TABLE test.schemas (id Int, format_schema String) ENGINE=MergeTree ORDER BY id"
        )
        instance.query(
            f"INSERT INTO test.schemas VALUES(1, '{schema_content.encode('utf-8').hex()}')"
        )
        return "SELECT unhex(format_schema) FROM test.schemas WHERE id=1"

    raise Exception(f"Invalid format_schema_source {format_schema_source}")


def test_protobuf_format_input_with_old_setting(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.http_query(
        "INSERT INTO test.simple SETTINGS format_schema='simple:KeyValuePair' FORMAT Protobuf",
        "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
    )
    assert instance.query("SELECT * from test.simple") == "1\tabc\n2\tdef\n"

    with pytest.raises(Exception) as exc:
        instance.http_query(
            "INSERT INTO test.simple SETTINGS format_schema='simple:KeyValuePair' format_schema_message_name='KeyValuePair' FORMAT Protobuf",
            "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
        )

    with pytest.raises(Exception) as exc:
        instance.http_query(
            "INSERT INTO test.simple SETTINGS format_schema='simple:KeyValuePair' format_schema_message_name='Tmp' FORMAT Protobuf",
            "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
        )
    with pytest.raises(Exception) as exc:
        instance.http_query(
            "INSERT INTO test.simple SETTINGS format_schema='simple:' format_schema_message_name='Tmp' FORMAT Protobuf",
            "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
        )

    instance.query("DROP DATABASE IF EXISTS test SYNC")

def test_protobuf_format_output_with_old_setting(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")
    assert (
        instance.http_query(
            "SELECT * FROM test.simple FORMAT Protobuf SETTINGS format_schema='simple:KeyValuePair'"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )

    instance.query("DROP DATABASE IF EXISTS test SYNC")


@pytest.mark.parametrize("format_schema_source", ["file", "string", "query"])
def test_protobuf_format_input_with_format_schema_source(started_cluster, format_schema_source : str):
    schema = """
syntax = "proto3";

message KeyValuePair {
    uint64 key = 1;
    string value = 2;
}
"""
    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    format_schema = get_format_schema(format_schema_source, schema)
    instance.http_query(
        f"INSERT INTO test.simple SETTINGS format_schema_source='{format_schema_source}', format_schema='{format_schema}', format_schema_message_name='KeyValuePair' FORMAT Protobuf",
        "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def",
    )
    assert instance.query("SELECT * from test.simple") == "1\tabc\n2\tdef\n"

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    if format_schema_source == "file":
        os.remove(os.path.join(database_path, "format_schemas", format_schema))

@pytest.mark.parametrize("format_schema_source", ["file", "string", "query"])
def test_protobuf_format_output_with_format_schema_source(started_cluster, format_schema_source : str):
    schema = """
syntax = "proto3";

message KeyValuePair {
    uint64 key = 1;
    string value = 2;
}
"""
    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")

    format_schema = get_format_schema(format_schema_source, schema)
    assert (
        instance.http_query(
            f"SELECT * FROM test.simple SETTINGS format_schema_source='{format_schema_source}', format_schema='{format_schema}', format_schema_message_name='KeyValuePair' FORMAT Protobuf"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    if format_schema_source == "file":
        os.remove(os.path.join(database_path, "format_schemas", format_schema))


def test_protobuf_format_output_with_format_schema_source_clear_cache_files(
    started_cluster,
):
    schema = """
syntax = "proto3";

message KeyValuePair {
    uint64 key = 1;
    string value = 2;
}
"""

    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")

    format_schema = get_format_schema("query", schema)
    format_schema_content = instance.query(format_schema)
    assert (
        instance.http_query(
            f"SELECT * FROM test.simple SETTINGS format_schema_source='query', format_schema='{format_schema}', format_schema_message_name='KeyValuePair' FORMAT Protobuf"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )

    new_schema = """
syntax = "proto3";

message MessageTmp {
    uint64 key2 = 1;
    string value2 = 2;
}
"""
    new_format_schema = get_format_schema("query", new_schema)
    new_format_schema_content = instance.query(new_format_schema)

    assert new_format_schema == format_schema
    assert new_format_schema_content != format_schema_content

    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Protobuf")
    # Not clear cached file yet, still work as the old schema is still being used
    assert (
        instance.http_query(
            f"SELECT * FROM test.simple SETTINGS format_schema_source='query', format_schema='{format_schema}', format_schema_message_name='KeyValuePair' FORMAT Protobuf"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )

    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Files")
    # After clearing, not work with new schema
    with pytest.raises(Exception) as exc:
        instance.http_query(
            f"SELECT * FROM test.simple SETTINGS format_schema_source='query', format_schema='{format_schema}', format_schema_message_name='MessageTmp' FORMAT Protobuf"
        )

    instance.query("DROP DATABASE IF EXISTS test SYNC")


@pytest.mark.parametrize("format_schema_source", ["file", "string", "query"])
def test_drop_cache_protobuf_format_with_format_schema_source(
    started_cluster, format_schema_source: str
):

    schema = """
syntax = "proto3";

message MessageTmp {
    uint64 key = 1;
    string value = 2;
}
"""
    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")

    format_schema = get_format_schema(format_schema_source, schema, "message_tmp.proto")
    assert (
        instance.http_query(
            f"SELECT * FROM test.simple SETTINGS format_schema_source='{format_schema_source}', format_schema='{format_schema}', format_schema_message_name='MessageTmp' FORMAT Protobuf"
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
    new_format_schema = get_format_schema(format_schema_source, new_schema, "message_tmp.proto")

    instance.query("DROP TABLE IF EXISTS test.new_simple")
    instance.query(
        """
        CREATE TABLE test.new_simple (key2 UInt64, value2 String)
            ENGINE = MergeTree ORDER BY tuple();
        """
    )
    instance.query("INSERT INTO test.new_simple VALUES (1, 'abc'), (2, 'def')")

    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Protobuf")
    if format_schema_source != "file":
        instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Files")

    # Tets works with new scheme
    assert (
        instance.http_query(
            f"SELECT * FROM test.new_simple SETTINGS format_schema_source='{format_schema_source}', format_schema='{new_format_schema}', format_schema_message_name='MessageTmp' FORMAT Protobuf"
        )
        == "\x07\x08\x01\x12\x03abc\x07\x08\x02\x12\x03def"
    )
    # Tests that stop working with old scheme
    with pytest.raises(Exception) as exc:
        instance.http_query(
            "SELECT * FROM test.simple SETTINGS format_schema_source='{format_schema_source}', format_schema='{new_format_schema}', format_schema_message_name='MessageTmp' FORMAT Protobuf"
        )

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    if format_schema_source == "file":
        os.remove(os.path.join(database_path, "format_schemas", format_schema))


@pytest.mark.parametrize("format_schema_source", ["file", "string", "query"])
def test_drop_capn_proto_format_with_format_schema_source(
    started_cluster, format_schema_source: str
):
    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    create_simple_table()
    instance.query("INSERT INTO test.simple VALUES (1, 'abc'), (2, 'def')")
    capn_proto_schema = """
@0x801f030c2b67bf19;

struct MessageTmp {
    key @0 :UInt64;
    value @1 :Text;
}
"""
    format_schema = get_format_schema(
        format_schema_source, capn_proto_schema, "message_tmp.capnp"
    )

    database_path = os.path.abspath(os.path.join(instance.path, "database"))
    format_schemas_path = os.path.join(database_path, "format_schemas")

    assert instance.http_query(
        f"SELECT * FROM test.simple FORMAT CapnProto SETTINGS format_schema_source='{format_schema_source}', format_schema='{format_schema}', format_schema_message_name='MessageTmp'"
    ) == instance.query(
        f"SELECT * FROM test.simple Format CapnProto SETTINGS format_schema='{format_schemas_path}/simple:MessageTmp'"
    )

    instance.query("SYSTEM CLEAR FORMAT SCHEMA CACHE")

    new_schema = """
@0x801f030c2b67bf19;

struct MessageTmp {
    key2 @0 :UInt64;
    value2 @1 :Text;
}
"""
    new_format_schema = get_format_schema(
        format_schema_source, new_schema, "message_tmp.capnp"
    )

    instance.query("DROP TABLE IF EXISTS test.new_simple")
    instance.query(
        """
        CREATE TABLE test.new_simple (key2 UInt64, value2 String)
            ENGINE = MergeTree ORDER BY tuple();
        """
    )
    instance.query("INSERT INTO test.new_simple VALUES (1, 'abc'), (2, 'def')")

    # Tets works with new scheme
    assert instance.http_query(
        f"SELECT * FROM test.new_simple FORMAT CapnProto SETTINGS format_schema_source='{format_schema_source}', format_schema='{new_format_schema}', format_schema_message_name='MessageTmp'"
    ) == instance.query(
        f"SELECT * FROM test.new_simple Format CapnProto SETTINGS format_schema='{format_schemas_path}/new_simple:MessageTmp'"
    )
    # Tests that stop working with old scheme
    with pytest.raises(Exception) as exc:
        instance.http_query(
            f"SELECT * FROM test.simple FORMAT CapnProto SETTINGS format_schema_source='{format_schema_source}', format_schema='{new_format_schema}', format_schema_message_name='MessageTmp'"
        )
    assert (
        "Capnproto schema doesn't contain field with name key. (THERE_IS_NO_COLUMN)"
        in str(exc.value)
    )
