# coding: utf-8

import base64
import os
import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string
from .flight_sql_client import (
    FlightSQLClient,
    flight_descriptor,
    ActionCreatePreparedStatementRequest,
    CommandStatementUpdate,
    DoPutUpdateResult,
    CancelStatus,
    SetSessionOptionsResult,
    CommandStatementQuery,
    CommandStatementIngest,
)


from helpers.cluster import ClickHouseCluster, get_docker_compose_path


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
    ],
)

session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

FLIGHT_SQL_TYPE_NAME = b"ARROW:FLIGHT:SQL:TYPE_NAME"
FLIGHT_SQL_PRECISION = b"ARROW:FLIGHT:SQL:PRECISION"
FLIGHT_SQL_SCALE = b"ARROW:FLIGHT:SQL:SCALE"
CLICKHOUSE_TYPE_NAME = b"CLICKHOUSE:TYPE_NAME"


def _field_metadata(field):
    return field.metadata or {}


def _ambiguous_type_query(where_clause=""):
    return f"""
        SELECT
            toDate('2024-01-02') AS date_col,
            toDate32('2024-01-02') AS date32_col,
            toInt8(1) AS int8_col,
            CAST('one' AS Enum8('one' = 1, 'two' = 2)) AS enum8_col,
            toInt16(2) AS int16_col,
            CAST('one' AS Enum16('one' = 1, 'two' = 2)) AS enum16_col,
            toUInt32(3) AS uint32_col,
            toDateTime('2024-01-02 03:04:05', 'UTC') AS datetime_col
        {where_clause}
    """


def _assert_schema_equal_with_metadata(actual, expected):
    assert actual.equals(expected, check_metadata=True), f"{actual}\n!=\n{expected}"


def get_client(session_id_override=None):
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        metadata={'x-clickhouse-session-id': session_id_override or session_id},
        features={'metadata-reflection': 'true'}, # makes the client emit metadata retrieval commands upon connection
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS mytable, map_test, large_test, bulk_test SYNC")


def test_select():
    client = get_client()
    flight_info = client.execute("SELECT 1, 'hello', 3.14")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()
    tsv_output = table.to_pandas().to_csv(sep='\t', index=False, header=False)

    assert tsv_output == "1\thello\t3.14\n"

def test_create_table_and_insert():
    client = get_client()

    # Create table
    client.execute_update("CREATE TABLE mytable (id UInt32, name String, value Float64) ENGINE = Memory")

    # Insert data
    client.execute_update("INSERT INTO mytable VALUES (1, 'test', 42.5), (2, 'hello', 3.14)")

    # Query and verify
    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    pandas_df = table.to_pandas()
    tsv_output = pandas_df.to_csv(sep='\t', index=False, header=False)

    expected = "1\ttest\t42.5\n2\thello\t3.14\n"
    assert tsv_output == expected


def test_map_data_type():
    client = get_client()

    # Test Map data type handling
    client.execute_update("CREATE TABLE map_test (id UInt32, data Map(String, UInt64)) ENGINE = Memory")
    client.execute_update("INSERT INTO map_test VALUES (1, {'key1': 100, 'key2': 200})")

    flight_info = client.execute("SELECT * FROM map_test")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    # Verify we can read the map data without errors
    assert table.num_rows == 1
    assert table.num_columns == 2

    # Check that the map column has the correct Arrow type
    map_column = table.column(1)
    assert isinstance(map_column.type, pa.MapType)


def test_error_handling():
    client = get_client()

    # Test invalid SQL
    with pytest.raises(flight.FlightServerError):
        client.execute("INVALID SQL SYNTAX")

    # Test querying non-existent table
    with pytest.raises(flight.FlightServerError):
        client.execute("SELECT * FROM non_existent_table")


def test_large_result_set():
    client = get_client()

    # Create table with many rows to test streaming
    client.execute_update("CREATE TABLE large_test (id UInt32, value String) ENGINE = Memory")
    client.execute_update("INSERT INTO large_test SELECT number, toString(number) FROM numbers(10000)")

    flight_info = client.execute("SELECT COUNT(*) FROM large_test")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    count_value = table.column(0)[0].as_py()
    assert count_value == 10000


def test_streaming_insert():
    """
    Test bulk data insertion via Arrow Flight SQL.

    Note: This test uses a workaround due to Arrow Flight SQL version limitations.
    Arrow Flight SQL v11 lacks bulk ingestion functionality (CommandStatementIngest),
    which was introduced in v12. ClickHouse supports a non-standard approach using
    CommandStatementUpdate, but this is not supported by the flightsql-dbapi module.

    This implementation uses a mix of the underlying Flight API with the Flight SQL
    protobuf definitions. When upgrading to Arrow Flight SQL v12+, this test should
    be replaced with the standard CommandStatementIngest approach.
    """
    client = get_client()

    client.execute_update("CREATE TABLE bulk_test (id UInt32, str String) ENGINE = Memory")

    cmd = CommandStatementUpdate(query="INSERT INTO bulk_test FORMAT Arrow")
    descriptor = flight_descriptor(cmd)
    schema = pa.schema([
        ("id", pa.uint32()),
        ("str", pa.string()),
    ])

    writer, reader = client.client.do_put(descriptor, schema, client._flight_call_options())

    for n in range(1000):
        batch = pa.record_batch([
            pa.array([n*1, n*2, n*3, n*4, n*5, n*6, n*7], type=pa.uint32()),
            pa.array([str(n*1), str(n*2), str(n*3), str(n*4), str(n*5), str(n*6), str(n*7)], type=pa.string()),
        ], schema=schema)
        writer.write_batch(batch)

    writer.done_writing()

    result = reader.read()

    assert result is not None
    update_result = DoPutUpdateResult()
    update_result.ParseFromString(result.to_pybytes())
    assert update_result.record_count == 7000


#
# Flight SQL Metadata Commands
#

def test_get_sql_info():
    """CommandGetSqlInfo returns server metadata."""
    client = get_client()
    flight_info = client.get_sql_info()
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    # Should have info_name (uint32) and value (dense_union) columns
    assert table.num_columns == 2
    assert table.column_names == ["info_name", "value"]
    assert table.num_rows > 0

    # Convert to dict for easier assertions
    info = {}
    for i in range(table.num_rows):
        info[table.column("info_name")[i].as_py()] = table.column("value")[i].as_py()

    # FLIGHT_SQL_SERVER_NAME = 0
    assert info[0] == "ClickHouse"
    # FLIGHT_SQL_SERVER_READ_ONLY = 3
    assert info[3] == False
    # FLIGHT_SQL_SERVER_SQL = 4
    assert info[4] == True
    # FLIGHT_SQL_SERVER_SUBSTRAIT = 5
    assert info[5] == False
    # FLIGHT_SQL_SERVER_CANCEL = 9
    assert info[9] == True


def test_get_sql_info_filtered():
    """CommandGetSqlInfo with specific info IDs returns only requested items."""
    client = get_client()
    # Request only FLIGHT_SQL_SERVER_NAME (0) and FLIGHT_SQL_SERVER_VERSION (1)
    flight_info = client.get_sql_info(info_ids=[0, 1])
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 2


def test_get_xdbc_type_info():
    """CommandGetXdbcTypeInfo returns ODBC type metadata for ClickHouse types."""
    client = get_client()
    flight_info = client.get_xdbc_type_info()
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    expected_columns = [
        "type_name",
        "data_type",
        "column_size",
        "literal_prefix",
        "literal_suffix",
        "create_params",
        "nullable",
        "case_sensitive",
        "searchable",
        "unsigned_attribute",
        "fixed_prec_scale",
        "auto_increment",
        "local_type_name",
        "minimum_scale",
        "maximum_scale",
        "sql_data_type",
        "datetime_subcode",
        "num_prec_radix",
        "interval_precision",
    ]
    assert table.num_columns == 19
    assert table.column_names == expected_columns
    assert table.schema.field("type_name").type == pa.utf8()
    assert table.schema.field("data_type").type == pa.int32()
    assert table.schema.field("case_sensitive").type == pa.bool_()
    assert table.schema.field("create_params").type == pa.list_(pa.field("item", pa.utf8(), nullable=False))
    type_names = [table.column("type_name")[i].as_py() for i in range(table.num_rows)]
    assert type_names == [
        "UUID",
        "Bool",
        "Int8",
        "UInt8",
        "Int64",
        "UInt64",
        "FixedString",
        "Int128",
        "Int256",
        "UInt128",
        "UInt256",
        "Decimal",
        "Int32",
        "UInt32",
        "Int16",
        "UInt16",
        "Float32",
        "Float64",
        "Enum16",
        "Enum8",
        "String",
        "Date",
        "Date32",
        "DateTime",
        "DateTime64",
    ]

    # Protocol requires ordering by (data_type, type_name).
    order_keys = [
        (table.column("data_type")[i].as_py(), table.column("type_name")[i].as_py())
        for i in range(table.num_rows)
    ]
    assert order_keys == sorted(order_keys)

    rows = {
        table.column("type_name")[i].as_py(): {
            name: table.column(name)[i].as_py() for name in expected_columns
        }
        for i in range(table.num_rows)
    }

    # Datetime rows report the generic SQL_DATETIME (9) in sql_data_type and the
    # concise type in datetime_subcode (1 = date, 3 = timestamp); other rows
    # repeat data_type in sql_data_type and have NULL datetime_subcode.
    for name in ("Date", "Date32"):
        assert rows[name]["data_type"] == 91
        assert rows[name]["sql_data_type"] == 9
        assert rows[name]["datetime_subcode"] == 1
    for name in ("DateTime", "DateTime64"):
        assert rows[name]["data_type"] == 93
        assert rows[name]["sql_data_type"] == 9
        assert rows[name]["datetime_subcode"] == 3
    assert rows["DateTime"]["minimum_scale"] == 0
    assert rows["DateTime"]["maximum_scale"] == 0
    assert rows["DateTime64"]["minimum_scale"] == 0
    assert rows["DateTime64"]["maximum_scale"] == 9
    assert rows["Int32"]["sql_data_type"] == 4
    assert rows["Int32"]["datetime_subcode"] is None

    # create_params is NULL for types without parameters and lists the
    # parameter keywords otherwise.
    assert rows["FixedString"]["create_params"] == ["length"]
    assert rows["Decimal"]["create_params"] == ["precision", "scale"]
    assert rows["DateTime"]["create_params"] == ["timezone"]
    assert rows["DateTime64"]["create_params"] == ["precision", "timezone"]
    assert rows["Int32"]["create_params"] is None
    assert rows["String"]["create_params"] is None

    # Exact numeric types report num_prec_radix = 10, approximate numerics 2.
    assert rows["Int32"]["num_prec_radix"] == 10
    assert rows["UInt64"]["num_prec_radix"] == 10
    assert rows["Float64"]["num_prec_radix"] == 2
    assert rows["String"]["num_prec_radix"] is None

    # Only character-like types support all predicates, including LIKE.
    assert rows["String"]["searchable"] == 3
    assert rows["FixedString"]["searchable"] == 3
    assert rows["Enum8"]["searchable"] == 3
    assert rows["Int32"]["searchable"] == 2
    assert rows["Date"]["searchable"] == 2

    # Enum values use quoted, case-sensitive string literals.
    for name in ("Enum8", "Enum16"):
        assert rows[name]["literal_prefix"] == "'"
        assert rows[name]["literal_suffix"] == "'"
        assert rows[name]["case_sensitive"] is True

    # Optional numeric attributes are NULL for non-numeric types.
    assert rows["Int32"]["unsigned_attribute"] is False
    assert rows["UInt32"]["unsigned_attribute"] is True
    assert rows["Int32"]["auto_increment"] is False
    assert rows["String"]["unsigned_attribute"] is None
    assert rows["String"]["auto_increment"] is None

    for name in ("FixedString", "Enum8", "Enum16"):
        assert rows[name]["column_size"] == 0xFFFFFF
    # Arrow UTF-8 arrays use signed 32-bit offsets and reserve one value.
    assert rows["String"]["column_size"] == 2**31 - 2

    schema_result = client.get_xdbc_type_info_schema()
    assert schema_result.schema == table.schema


def test_get_xdbc_type_info_filtered():
    """CommandGetXdbcTypeInfo with data_type filter returns matching rows only."""
    client = get_client()
    # SQL_INTEGER = 4 -> Int32 / UInt32
    flight_info = client.get_xdbc_type_info(data_type=4)
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    type_names = [table.column("type_name")[i].as_py() for i in range(table.num_rows)]
    assert type_names == ["Int32", "UInt32"]
    assert all(table.column("data_type")[i].as_py() == 4 for i in range(table.num_rows))
    assert [
        (table.column("data_type")[i].as_py(), table.column("type_name")[i].as_py())
        for i in range(table.num_rows)
    ] == sorted(
        (table.column("data_type")[i].as_py(), table.column("type_name")[i].as_py())
        for i in range(table.num_rows)
    )
    _assert_schema_equal_with_metadata(
        table.schema, client.get_xdbc_type_info_schema(data_type=4).schema
    )

    # Unknown ODBC code -> empty table with full schema.
    flight_info = client.get_xdbc_type_info(data_type=999)
    reader = client.do_get(flight_info.endpoints[0].ticket)
    empty = reader.read_all()
    assert empty.num_rows == 0
    assert empty.num_columns == 19
    _assert_schema_equal_with_metadata(
        empty.schema, client.get_xdbc_type_info_schema(data_type=999).schema
    )


def test_get_catalogs():
    """CommandGetCatalogs returns empty result (ClickHouse has no catalogs)."""
    client = get_client()
    flight_info = client.get_catalogs()
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 0
    assert "catalog_name" in table.column_names


def test_get_db_schemas():
    """CommandGetDbSchemas returns database list."""
    client = get_client()
    flight_info = client.get_db_schemas()
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    schemas = [table.column("db_schema_name")[i].as_py() for i in range(table.num_rows)]
    assert "default" in schemas
    assert "system" in schemas


def test_get_db_schemas_with_filter():
    """CommandGetDbSchemas with filter pattern."""
    client = get_client()
    flight_info = client.get_db_schemas(db_schema_filter_pattern="def%")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    schemas = [table.column("db_schema_name")[i].as_py() for i in range(table.num_rows)]
    assert "default" in schemas
    assert "system" not in schemas


def test_get_tables():
    """CommandGetTables returns table list."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32) ENGINE = Memory")

    flight_info = client.get_tables(
        db_schema_filter_pattern="default",
        table_name_filter_pattern="mytable"
    )
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    assert table.column("table_name")[0].as_py() == "mytable"


def test_get_tables_with_schema():
    """CommandGetTables with include_schema=True returns Arrow schema bytes."""
    client = get_client()
    client.execute_update(
        "CREATE TABLE mytable ("
        "date_col Date, date32_col Date32, int8_col Int8, "
        "enum8_col Enum8('one' = 1, 'two' = 2), uint32_col UInt32, "
        "datetime_col DateTime('UTC'), decimal_col Decimal(18, 4), "
        "nullable_col Nullable(String)) ENGINE = Memory"
    )

    flight_info = client.get_tables(
        db_schema_filter_pattern="default",
        table_name_filter_pattern="mytable",
        include_schema=True
    )
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    assert "table_schema" in table.column_names
    schema_bytes = table.column("table_schema")[0].as_py()
    table_schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))

    expected = {
        "date_col": (pa.date32(), b"Date", b"Date"),
        "date32_col": (pa.date32(), b"Date32", b"Date32"),
        "int8_col": (pa.int8(), b"Int8", b"Int8"),
        "enum8_col": (pa.int8(), b"Enum8", b"Enum8('one' = 1, 'two' = 2)"),
        "uint32_col": (pa.uint32(), b"UInt32", b"UInt32"),
        "datetime_col": (pa.uint32(), b"DateTime", b"DateTime('UTC')"),
        "decimal_col": (pa.decimal128(18, 4), b"Decimal", b"Decimal(18, 4)"),
        "nullable_col": (pa.string(), b"String", b"Nullable(String)"),
    }
    assert table_schema.names == list(expected)
    for name, (arrow_type, type_name, clickhouse_type_name) in expected.items():
        field = table_schema.field(name)
        metadata = _field_metadata(field)
        assert field.type == arrow_type
        assert metadata[FLIGHT_SQL_TYPE_NAME] == type_name
        assert metadata[CLICKHOUSE_TYPE_NAME] == clickhouse_type_name

    assert _field_metadata(table_schema.field("decimal_col"))[FLIGHT_SQL_PRECISION] == b"18"
    assert _field_metadata(table_schema.field("decimal_col"))[FLIGHT_SQL_SCALE] == b"4"

    # The outer protocol schema remains fixed and does not inherit inner table metadata.
    assert all(FLIGHT_SQL_TYPE_NAME not in _field_metadata(field) for field in table.schema)

    without_schema_info = client.get_tables(
        db_schema_filter_pattern="default",
        table_name_filter_pattern="mytable",
        include_schema=False,
    )
    without_schema = client.do_get(without_schema_info.endpoints[0].ticket).read_all()
    assert without_schema.column_names == [
        "catalog_name",
        "db_schema_name",
        "table_name",
        "table_type",
    ]


def test_get_table_types():
    """CommandGetTableTypes returns engine types."""
    client = get_client()
    flight_info = client.get_table_types()
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    types = [table.column("table_type")[i].as_py() for i in range(table.num_rows)]
    assert "REMOTE TABLE" in types
    assert "VIEW" in types
    assert "UNKNOWN TABLE TYPE" not in types, \
        "Some engine(s) in system.table_engines are not mapped in engine_to_type (commandSelector.cpp)"


@pytest.mark.parametrize("where_clause", ["", "WHERE 0"])
def test_statement_query_type_metadata(where_clause):
    """Statement schemas expose ClickHouse type identity on every protocol path."""
    client = get_client()
    query = _ambiguous_type_query(where_clause)

    schema_from_get_schema = client.get_schema(query).schema
    flight_info = client.execute(query)
    schema_from_flight_info = flight_info.schema
    table = client.do_get(flight_info.endpoints[0].ticket).read_all()

    _assert_schema_equal_with_metadata(schema_from_get_schema, schema_from_flight_info)
    _assert_schema_equal_with_metadata(schema_from_flight_info, table.schema)
    assert table.num_rows == (0 if where_clause else 1)

    expected = {
        "date_col": (pa.date32(), b"Date", b"Date"),
        "date32_col": (pa.date32(), b"Date32", b"Date32"),
        "int8_col": (pa.int8(), b"Int8", b"Int8"),
        "enum8_col": (pa.int8(), b"Enum8", b"Enum8('one' = 1, 'two' = 2)"),
        "int16_col": (pa.int16(), b"Int16", b"Int16"),
        "enum16_col": (pa.int16(), b"Enum16", b"Enum16('one' = 1, 'two' = 2)"),
        "uint32_col": (pa.uint32(), b"UInt32", b"UInt32"),
        "datetime_col": (pa.uint32(), b"DateTime", b"DateTime('UTC')"),
    }
    for name, (arrow_type, type_name, clickhouse_type_name) in expected.items():
        field = table.schema.field(name)
        metadata = _field_metadata(field)
        assert field.type == arrow_type
        assert metadata[FLIGHT_SQL_TYPE_NAME] == type_name
        assert metadata[CLICKHOUSE_TYPE_NAME] == clickhouse_type_name

    if not where_clause:
        type_info = client.do_get(client.get_xdbc_type_info().endpoints[0].ticket).read_all()
        catalog_names = [
            type_info.column("type_name")[i].as_py() for i in range(type_info.num_rows)
        ]
        for _, type_name, _ in expected.values():
            assert catalog_names.count(type_name.decode()) == 1


def test_wrapped_and_parameterized_type_metadata():
    """Wrappers are removed only from the standard type family metadata."""
    client = get_client()
    query = """
        SELECT
            CAST(NULL AS Nullable(Date32)) AS nullable_date32,
            CAST('value' AS LowCardinality(String)) AS low_cardinality_string,
            CAST('value' AS LowCardinality(Nullable(String))) AS nullable_low_cardinality_string,
            CAST('12.3456' AS Decimal(18, 4)) AS decimal_col,
            CAST('fixed' AS FixedString(17)) AS fixed_string_col,
            toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC') AS datetime64_col,
            CAST('two' AS Enum8('one' = 1, 'two' = 2, 'three' = 3)) AS enum_col,
            toBool(1) AS bool_col
    """
    flight_info = client.execute(query)
    schema = client.do_get(flight_info.endpoints[0].ticket).read_all().schema

    expected = {
        "nullable_date32": (b"Date32", b"Nullable(Date32)"),
        "low_cardinality_string": (b"String", b"LowCardinality(String)"),
        "nullable_low_cardinality_string": (
            b"String",
            b"LowCardinality(Nullable(String))",
        ),
        "decimal_col": (b"Decimal", b"Decimal(18, 4)"),
        "fixed_string_col": (b"FixedString", b"FixedString(17)"),
        "datetime64_col": (b"DateTime64", b"DateTime64(6, 'UTC')"),
        "enum_col": (b"Enum8", b"Enum8('one' = 1, 'two' = 2, 'three' = 3)"),
        "bool_col": (b"Bool", b"Bool"),
    }
    for name, (type_name, clickhouse_type_name) in expected.items():
        metadata = _field_metadata(schema.field(name))
        assert metadata[FLIGHT_SQL_TYPE_NAME] == type_name
        assert metadata[CLICKHOUSE_TYPE_NAME] == clickhouse_type_name

    decimal_metadata = _field_metadata(schema.field("decimal_col"))
    assert decimal_metadata[FLIGHT_SQL_PRECISION] == b"18"
    assert decimal_metadata[FLIGHT_SQL_SCALE] == b"4"
    assert _field_metadata(schema.field("fixed_string_col"))[FLIGHT_SQL_PRECISION] == b"17"
    datetime64_metadata = _field_metadata(schema.field("datetime64_col"))
    assert datetime64_metadata[FLIGHT_SQL_PRECISION] == b"26"
    assert datetime64_metadata[FLIGHT_SQL_SCALE] == b"6"
    assert _field_metadata(schema.field("bool_col"))[FLIGHT_SQL_PRECISION] == b"1"


def test_type_metadata_preserves_existing_metadata():
    """Adding Flight SQL metadata preserves the Arrow UUID extension."""
    client = get_client()
    flight_info = client.execute(
        "SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS uid"
    )
    field = client.do_get(flight_info.endpoints[0].ticket).read_all().schema.field("uid")
    metadata = _field_metadata(field)

    assert metadata[FLIGHT_SQL_TYPE_NAME] == b"UUID"
    assert metadata[CLICKHOUSE_TYPE_NAME] == b"UUID"
    assert metadata[b"ARROW:extension:name"] == b"arrow.uuid"
    assert metadata[b"ARROW:extension:metadata"] == b""
    assert metadata[b"PARQUET:logical_type"] == b"UUID"


def test_unsupported_type_metadata_policy():
    """Complex types keep their ClickHouse name without claiming an XDBC row."""
    client = get_client()
    query = """
        SELECT
            CAST([1, 2] AS Array(UInt32)) AS array_col,
            CAST(map('one', 1) AS Map(String, UInt32)) AS map_col,
            CAST((1, 'one') AS Tuple(Int8, String)) AS tuple_col,
            toIPv4('192.0.2.1') AS ipv4_col
    """
    schema = client.do_get(client.execute(query).endpoints[0].ticket).read_all().schema

    assert pa.types.is_list(schema.field("array_col").type)
    assert schema.field("array_col").type.value_type == pa.uint32()
    assert pa.types.is_map(schema.field("map_col").type)
    assert schema.field("map_col").type.key_type == pa.string()
    assert schema.field("map_col").type.item_type == pa.uint32()
    assert pa.types.is_struct(schema.field("tuple_col").type)
    assert [field.type for field in schema.field("tuple_col").type] == [
        pa.int8(),
        pa.string(),
    ]

    expected = {
        "array_col": b"Array(UInt32)",
        "map_col": b"Map(String, UInt32)",
        "tuple_col": b"Tuple(Int8, String)",
    }
    for name, clickhouse_type_name in expected.items():
        field = schema.field(name)
        metadata = _field_metadata(field)
        assert FLIGHT_SQL_TYPE_NAME not in metadata
        assert metadata[CLICKHOUSE_TYPE_NAME] == clickhouse_type_name

    ipv4_metadata = _field_metadata(schema.field("ipv4_col"))
    assert FLIGHT_SQL_TYPE_NAME not in ipv4_metadata
    assert ipv4_metadata[CLICKHOUSE_TYPE_NAME] == b"IPv4"

    type_info = client.do_get(client.get_xdbc_type_info().endpoints[0].ticket).read_all()
    catalog_names = {
        type_info.column("type_name")[i].as_py() for i in range(type_info.num_rows)
    }
    assert catalog_names.isdisjoint({"Array", "Map", "Tuple", "IPv4"})


def test_type_metadata_is_limited_to_flight_sql_queries():
    """Raw Arrow Flight descriptors do not receive Flight SQL column metadata."""
    client = get_client()
    descriptor = flight.FlightDescriptor.for_command(b"SELECT toDate32('2024-01-02') AS value")
    info = client.client.get_flight_info(descriptor, client._flight_call_options())
    table = client.do_get(info.endpoints[0].ticket).read_all()
    assert FLIGHT_SQL_TYPE_NAME not in _field_metadata(table.schema.field("value"))
    assert CLICKHOUSE_TYPE_NAME not in _field_metadata(table.schema.field("value"))


def test_get_primary_keys():
    """CommandGetPrimaryKeys returns primary key columns."""
    client = get_client()
    client.execute_update(
        "CREATE TABLE mytable (id UInt32, name String, value Float64) ENGINE = MergeTree ORDER BY (id, name)"
    )

    flight_info = client.get_primary_keys(table="mytable", db_schema="default")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 2
    columns = [table.column("column_name")[i].as_py() for i in range(table.num_rows)]
    assert columns == ["id", "name"]
    # key_seq should be 1-based sequential
    seqs = [table.column("key_seq")[i].as_py() for i in range(table.num_rows)]
    assert seqs == [1, 2]


#
# DoAction Tests
#

def test_set_session_options():
    """SetSessionOptions sets ClickHouse settings."""
    client = get_client()
    result = client.set_session_options({"max_threads": "4"})
    assert len(result.errors) == 0


def test_set_session_options_invalid_setting():
    """SetSessionOptions with unknown setting returns INVALID_NAME error."""
    client = get_client()
    result = client.set_session_options({"nonexistent_setting_xyz": "value"})
    assert "nonexistent_setting_xyz" in result.errors
    assert result.errors["nonexistent_setting_xyz"].value == SetSessionOptionsResult.INVALID_NAME


def test_get_session_options():
    """GetSessionOptions returns current settings."""
    client = get_client()
    result = client.get_session_options()
    assert "max_threads" in result.session_options
    assert result.session_options["max_threads"].string_value != ""


def _query_setting(client, name):
    """Read the current value of a setting via SQL query."""
    flight_info = client.execute(f"SELECT value FROM system.settings WHERE name = '{name}'")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()
    return table.column(0)[0].as_py()


def test_set_session_options_persistence():
    """SetSessionOptions changes persist and are visible in subsequent queries."""
    client = get_client()

    # Reset max_threads to default first (previous tests may have modified it)
    result = client.set_session_options({"max_threads": None})
    assert len(result.errors) == 0

    # Read the default value
    default_value = _query_setting(client, "max_threads")

    # Pick a value that differs from the default
    new_value = "7" if default_value != "7" else "5"

    # Set the setting via SetSessionOptions
    result = client.set_session_options({"max_threads": new_value})
    assert len(result.errors) == 0

    # Verify the setting persists via SQL query
    assert _query_setting(client, "max_threads") == new_value

    # Verify via GetSessionOptions as well
    options = client.get_session_options()
    assert options.session_options["max_threads"].string_value == new_value

    # Reset to default
    result = client.set_session_options({"max_threads": None})
    assert len(result.errors) == 0

    # Verify the setting was restored to the original default
    assert _query_setting(client, "max_threads") == default_value


def test_reset_session_option_respects_settings_constraints():
    constraint_session_id = 'settings_constraints_' + ''.join(
        random.choices(string.ascii_letters + string.digits, k=16)
    )
    client = get_client(constraint_session_id)

    result = client.set_session_options({"readonly": "2"})
    assert len(result.errors) == 0

    result = client.set_session_options({"readonly": None})
    assert "readonly" in result.errors

    assert _query_setting(client, "readonly") == "2"


def test_cancel_flight_info():
    client = get_client()

    descriptor = flight.FlightDescriptor.for_command(
        b"SELECT sleepEachRow(0.5) FROM numbers(100)"
    )
    poll_result = client.poll_flight_info(descriptor)
    assert poll_result.info is not None

    result = client.cancel_flight_info(poll_result.info_bytes)
    assert result.status == CancelStatus.Value('CANCEL_STATUS_CANCELLED')


def test_unsupported_action():
    """Unsupported action type returns error."""
    client = get_client()
    action = flight.Action("SomeUnsupportedAction", b"")
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="not supported"):
        list(client.client.do_action(action, client._flight_call_options()))


#
# PollFlightInfo Tests
#

def test_poll_flight_info_basic():
    """PollFlightInfo streams results incrementally."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable SELECT number FROM numbers(100)")

    descriptor = flight.FlightDescriptor.for_command(b"SELECT * FROM mytable")

    poll_result = client.poll_flight_info(descriptor)
    assert poll_result.info is not None

    # Collect all FlightInfo bytes by polling until no next descriptor
    all_infos = [poll_result.info]
    while poll_result.flight_descriptor is not None:
        poll_result = client.poll_flight_info(poll_result.flight_descriptor)
        all_infos.append(poll_result.info)

    # Read all data via tickets
    total_rows = 0
    for endpoint in all_infos[-1].endpoints:
        reader = client.do_get(endpoint.ticket)
        table = reader.read_all()
        total_rows += table.num_rows

    assert total_rows == 100


def test_poll_flight_info_type_metadata():
    """PollFlightInfo and its final DoGet stream expose identical metadata."""
    client = get_client()
    descriptor = flight_descriptor(CommandStatementQuery(query=_ambiguous_type_query()))

    poll_result = client.poll_flight_info(descriptor)
    infos = [poll_result.info]
    while poll_result.flight_descriptor is not None:
        poll_result = client.poll_flight_info(poll_result.flight_descriptor)
        infos.append(poll_result.info)

    assert infos[-1].endpoints
    table = client.do_get(infos[-1].endpoints[0].ticket).read_all()
    for info in infos:
        _assert_schema_equal_with_metadata(info.schema, table.schema)

    metadata = _field_metadata(table.schema.field("datetime_col"))
    assert metadata[FLIGHT_SQL_TYPE_NAME] == b"DateTime"
    assert metadata[CLICKHOUSE_TYPE_NAME] == b"DateTime('UTC')"


def test_poll_flight_info_with_path_descriptor():
    """PollFlightInfo works with PATH descriptor (table name)."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'a'), (2, 'b')")

    descriptor = flight.FlightDescriptor.for_path("mytable")

    poll_result = client.poll_flight_info(descriptor)
    assert poll_result.info is not None
    assert poll_result.info.total_records >= 0

    # Cancel the running query so cleanup can drop the table
    client.cancel_flight_info(poll_result.info_bytes)


#
# GetSchema Tests
#

def test_get_schema():
    """GetSchema returns schema without executing the query."""
    client = get_client()

    client.execute_update(
        "CREATE TABLE mytable (id UInt32, name String, value Float64) ENGINE = Memory"
    )

    # GetSchema via Flight SQL CommandStatementQuery
    schema_result = client.get_schema("SELECT * FROM mytable")
    schema = schema_result.schema

    assert len(schema) == 3
    assert schema.field("id").type == pa.uint32()
    assert schema.field("name").type == pa.string()
    assert schema.field("value").type == pa.float64()


def test_get_schema_path_descriptor():
    """GetSchema works with PATH descriptor."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id Int64, name String) ENGINE = Memory")

    descriptor = flight.FlightDescriptor.for_path("mytable")
    options = client._flight_call_options()

    schema_result = client.client.get_schema(descriptor, options)
    schema = schema_result.schema

    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()


#
# Data Type Coverage
#

def test_array_data_type():
    """Array type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, arr Array(UInt32)) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, [10, 20, 30])")

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    assert isinstance(table.column("arr").type, pa.ListType)
    assert table.column("arr")[0].as_py() == [10, 20, 30]


def test_tuple_data_type():
    """Tuple type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, t Tuple(String, UInt32)) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, ('hello', 42))")

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    # Tuple maps to Arrow struct
    assert isinstance(table.column("t").type, pa.StructType)


def test_nullable_data_type():
    """Nullable type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, val Nullable(String)) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'hello'), (2, NULL)")

    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 2
    assert table.column("val")[0].as_py() == "hello"
    assert table.column("val")[1].as_py() is None


def test_datetime_data_types():
    """DateTime and DateTime64 round-trip."""
    client = get_client()
    client.execute_update(
        "CREATE TABLE mytable (id UInt32, dt DateTime, dt64 DateTime64(3)) ENGINE = Memory"
    )
    client.execute_update(
        "INSERT INTO mytable VALUES (1, '2024-01-15 10:30:00', '2024-01-15 10:30:00.123')"
    )

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    # DateTime maps to uint32 (unix timestamp)
    assert table.column("dt").type == pa.uint32()
    assert table.column("dt")[0].as_py() == 1705314600
    # DateTime64 maps to Arrow timestamp
    assert pa.types.is_timestamp(table.column("dt64").type)

def test_decimal_data_type():
    """Decimal type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, val Decimal(18, 4)) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 123.4567)")

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1
    assert pa.types.is_decimal(table.column("val").type)


def test_uuid_data_type():
    """UUID type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, uid UUID) ENGINE = Memory")
    client.execute_update(
        "INSERT INTO mytable VALUES (1, '550e8400-e29b-41d4-a716-446655440000')"
    )

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 1


def test_lowcardinality_data_type():
    """LowCardinality type round-trip."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, val LowCardinality(String)) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'aaa'), (2, 'bbb'), (3, 'aaa')")

    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 3
    vals = [table.column("val")[i].as_py() for i in range(3)]
    assert vals == ["aaa", "bbb", "aaa"]


def test_enum_data_type():
    """Enum type round-trip."""
    client = get_client()
    client.execute_update(
        "CREATE TABLE mytable (id UInt32, status Enum8('ok' = 1, 'error' = 2)) ENGINE = Memory"
    )
    client.execute_update("INSERT INTO mytable VALUES (1, 'ok'), (2, 'error')")

    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 2


#
# Session Management
#

def test_session_state_persistence():
    """Session ID preserves state across requests (e.g., temp tables, settings)."""
    client = get_client()  # already uses x-clickhouse-session-id

    client.execute_update("SET max_threads = 2")

    flight_info = client.execute("SELECT value FROM system.settings WHERE name = 'max_threads'")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.column(0)[0].as_py() == "2"


def test_different_sessions_are_independent():
    """Different session IDs have independent state."""
    import random, string
    session_id_1 = ''.join(random.choices(string.ascii_letters, k=16))
    session_id_2 = ''.join(random.choices(string.ascii_letters, k=16))

    client1 = FlightSQLClient(
        host=node.ip_address, port=8888, insecure=True,
        disable_server_verification=True,
        metadata={'x-clickhouse-session-id': session_id_1},
    )
    client2 = FlightSQLClient(
        host=node.ip_address, port=8888, insecure=True,
        disable_server_verification=True,
        metadata={'x-clickhouse-session-id': session_id_2},
    )

    client1.execute_update("SET max_threads = 3")

    # client2 should still see the default
    flight_info = client2.execute("SELECT value FROM system.settings WHERE name = 'max_threads'")
    reader = client2.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    # Should NOT be "3" since it's a different session
    assert table.column(0)[0].as_py() != "3"


#
# Bearer Token Authentication
#

def test_bearer_token_reuse():
    """After Basic auth, the returned Bearer token can authenticate subsequent requests."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")

    # First request with Basic auth returns a Bearer token
    token_pair = client.authenticate_basic_token("default", "")
    options = flight.FlightCallOptions(headers=[token_pair])

    # Use the Bearer token for a query
    ticket = flight.Ticket(b"SELECT 1")
    reader = client.do_get(ticket, options)
    table = reader.read_all()
    assert table.column(0)[0].as_py() == 1


def test_basic_auth_without_padding():
    """Some clients (e.g. the Go Flight client used by the ADBC Flight SQL driver)
    send the Base64-encoded Basic credentials without the '=' padding."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")

    credentials = base64.b64encode(b"default:").decode().rstrip("=")
    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Basic {credentials}".encode())]
    )

    reader = client.do_get(flight.Ticket(b"SELECT 1"), options)
    table = reader.read_all()
    assert table.column(0)[0].as_py() == 1


def test_basic_auth_malformed_base64():
    """A malformed 'authorization' header must produce a clean authentication error,
    not a generic 'Unexpected error in RPC handling' from an exception escaping into gRPC."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")

    options = flight.FlightCallOptions(
        headers=[(b"authorization", b"Basic !!!not-base64!!!")]
    )

    with pytest.raises(
        flight.FlightUnauthenticatedError,
        match="Cannot decode the Base64-encoded credentials",
    ):
        client.do_get(flight.Ticket(b"SELECT 1"), options)


def test_basic_auth_without_credentials_separator():
    """Basic credentials without a username/password separator must not authenticate."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")

    credentials = base64.b64encode(b"default").decode().rstrip("=")
    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Basic {credentials}".encode())]
    )

    with pytest.raises(
        flight.FlightUnauthenticatedError,
        match="Malformed credentials in the 'authorization' header",
    ):
        client.do_get(flight.Ticket(b"SELECT 1"), options)


def test_unsupported_authorization_header():
    """An unsupported 'authorization' header must not fall through to default authentication."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")

    options = flight.FlightCallOptions(
        headers=[(b"authorization", b"Digest credentials")]
    )

    with pytest.raises(
        flight.FlightUnauthenticatedError,
        match="Unsupported 'authorization' header",
    ):
        client.do_get(flight.Ticket(b"SELECT 1"), options)


#
# Edge Cases
#

def test_empty_result_set():
    """Query returning zero rows produces valid empty table."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")

    flight_info = client.execute("SELECT * FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 0
    assert table.num_columns == 2
    assert table.schema.field("id").type == pa.uint32()
    assert table.schema.field("name").type == pa.string()


def test_empty_query_in_command_statement():
    """CommandStatementQuery with empty query returns error."""
    client = get_client()
    # Construct a CommandStatementQuery with empty query string
    cmd = CommandStatementQuery(query="")
    desc = flight_descriptor(cmd)
    options = client._flight_call_options()

    with pytest.raises(pa.lib.ArrowInvalid, match="query must not be empty"):
        client.client.get_flight_info(desc, options)


def test_multiple_statements_via_execute_update():
    """Multiple DDL/DML via execute_update in sequence."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, val String) ENGINE = Memory")

    for i in range(10):
        client.execute_update(f"INSERT INTO mytable VALUES ({i}, 'row_{i}')")

    flight_info = client.execute("SELECT count() FROM mytable")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.column(0)[0].as_py() == 10


def test_special_characters_in_data():
    """Data with special characters (unicode, quotes, newlines) round-trips correctly."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, val String) ENGINE = Memory")
    client.execute_update(
        r"INSERT INTO mytable VALUES (1, 'hello\nworld'), (2, 'it''s \"quoted\"'), (3, '日本語テスト')"
    )

    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 3
    assert table.column("val")[2].as_py() == '日本語テスト'


#
# CommandStatementIngest
#

def test_statement_ingest():
    """CommandStatementIngest inserts data into existing table."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")

    cmd = CommandStatementIngest()
    cmd.table = "mytable"
    cmd.table_definition_options.if_not_exist = (
        CommandStatementIngest.TableDefinitionOptions.TABLE_NOT_EXIST_OPTION_FAIL
    )
    cmd.table_definition_options.if_exists = (
        CommandStatementIngest.TableDefinitionOptions.TABLE_EXISTS_OPTION_APPEND
    )

    descriptor = flight_descriptor(cmd)
    schema = pa.schema([("id", pa.uint32()), ("name", pa.string())])

    writer, reader = client.client.do_put(descriptor, schema, client._flight_call_options())
    batch = pa.record_batch(
        [pa.array([1, 2, 3], type=pa.uint32()), pa.array(["a", "b", "c"], type=pa.string())],
        schema=schema,
    )
    writer.write_batch(batch)
    writer.done_writing()
    result = reader.read()
    writer.close()

    update_result = DoPutUpdateResult()
    update_result.ParseFromString(result.to_pybytes())
    assert update_result.record_count == 3

    # Verify data
    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    r = client.do_get(flight_info.endpoints[0].ticket)
    t = r.read_all()
    assert t.num_rows == 3


def test_statement_ingest_with_schema():
    """CommandStatementIngest with database schema prefix."""
    client = get_client()
    client.execute_update("CREATE TABLE default.mytable (id UInt32) ENGINE = Memory")

    cmd = CommandStatementIngest()
    cmd.table = "mytable"
    cmd.schema = "default"
    cmd.table_definition_options.if_not_exist = (
        CommandStatementIngest.TableDefinitionOptions.TABLE_NOT_EXIST_OPTION_FAIL
    )
    cmd.table_definition_options.if_exists = (
        CommandStatementIngest.TableDefinitionOptions.TABLE_EXISTS_OPTION_APPEND
    )

    descriptor = flight_descriptor(cmd)
    schema = pa.schema([("id", pa.uint32())])
    writer, reader = client.client.do_put(descriptor, schema, client._flight_call_options())
    batch = pa.record_batch([pa.array([1], type=pa.uint32())], schema=schema)
    writer.write_batch(batch)
    writer.done_writing()
    reader.read()
    writer.close()


def test_statement_ingest_catalog_not_supported():
    """CommandStatementIngest with catalog returns NotImplemented."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32) ENGINE = Memory")

    cmd = CommandStatementIngest()
    cmd.table = "mytable"
    cmd.catalog = "some_catalog"

    descriptor = flight_descriptor(cmd)
    schema = pa.schema([("id", pa.uint32())])

    with pytest.raises(pa.lib.ArrowNotImplementedError, match="Catalogs are not supported"):
        writer, reader = client.client.do_put(descriptor, schema, client._flight_call_options())
        batch = pa.record_batch([pa.array([1], type=pa.uint32())], schema=schema)
        writer.write_batch(batch)
        writer.close()


def test_statement_ingest_temporary_not_supported():
    """CommandStatementIngest with temporary=True returns NotImplemented."""
    client = get_client()
    client.execute_update("CREATE TABLE mytable (id UInt32) ENGINE = Memory")

    cmd = CommandStatementIngest()
    cmd.table = "mytable"
    cmd.temporary = True

    descriptor = flight_descriptor(cmd)
    schema = pa.schema([("id", pa.uint32())])

    with pytest.raises(pa.lib.ArrowNotImplementedError, match="Implicit temporary tables are not supported"):
        writer, reader = client.client.do_put(descriptor, schema, client._flight_call_options())
        batch = pa.record_batch([pa.array([1], type=pa.uint32())], schema=schema)
        writer.write_batch(batch)
        writer.close()


def test_prepared_statement_create_and_close():
    """CreatePreparedStatement validates SQL and returns dataset schema; ClosePreparedStatement cleans up."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String, value Float64) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'test', 42.5), (2, 'hello', 3.14)")

    stmt = client.prepare("SELECT id, name, value FROM mytable WHERE id = ?")

    # Schema should reflect the three result columns
    assert stmt.dataset_schema is not None
    assert len(stmt.dataset_schema) == 3
    assert stmt.dataset_schema.field(0).name == "id"
    assert stmt.dataset_schema.field(1).name == "name"
    assert stmt.dataset_schema.field(2).name == "value"

    # Handle should be non-empty
    assert len(stmt.handle) > 0

    # Close should not raise
    stmt.close()


def test_prepared_statement_type_metadata():
    """Prepared dataset, GetSchema, FlightInfo, and DoGet schemas stay identical."""
    client = get_client()
    stmt = client.prepare(_ambiguous_type_query("WHERE toUInt32(?) = 1"))

    try:
        assert stmt.dataset_schema is not None
        assert _field_metadata(stmt.dataset_schema.field("enum8_col"))[
            FLIGHT_SQL_TYPE_NAME
        ] == b"Enum8"

        stmt.bind_parameters(
            pa.record_batch([pa.array([1], type=pa.uint32())], names=["param_1"])
        )
        schema_from_get_schema = client.get_prepared_statement_schema(stmt.handle).schema
        flight_info = client.get_prepared_statement_flight_info(stmt.handle)
        table = client.do_get(flight_info.endpoints[0].ticket).read_all()

        _assert_schema_equal_with_metadata(stmt.dataset_schema, schema_from_get_schema)
        _assert_schema_equal_with_metadata(schema_from_get_schema, flight_info.schema)
        _assert_schema_equal_with_metadata(flight_info.schema, table.schema)
        assert table.num_rows == 1

        stmt.bind_parameters(
            pa.record_batch([pa.array([2], type=pa.uint32())], names=["param_1"])
        )
        rebound_schema = client.get_prepared_statement_schema(stmt.handle).schema
        rebound_info = client.get_prepared_statement_flight_info(stmt.handle)
        rebound_table = client.do_get(rebound_info.endpoints[0].ticket).read_all()

        _assert_schema_equal_with_metadata(stmt.dataset_schema, rebound_schema)
        _assert_schema_equal_with_metadata(rebound_schema, rebound_info.schema)
        _assert_schema_equal_with_metadata(rebound_info.schema, rebound_table.schema)
        assert rebound_table.num_rows == 0
    finally:
        stmt.close()


def test_prepared_statement_invalid_sql():
    """CreatePreparedStatement with invalid SQL should return an error."""
    client = get_client()

    with pytest.raises(flight.FlightServerError):
        client.prepare("SELEKT invalid syntax !!!")


def test_prepared_statement_no_params():
    """CreatePreparedStatement works for a query without placeholders."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32) ENGINE = Memory")

    stmt = client.prepare("SELECT id FROM mytable")

    assert stmt.dataset_schema is not None
    assert len(stmt.dataset_schema) == 1
    assert stmt.dataset_schema.field(0).name == "id"

    stmt.close()


def test_prepared_statement_empty_query():
    """CreatePreparedStatement with empty query returns an error."""
    client = get_client()

    with pytest.raises(pa.lib.ArrowInvalid, match="query must not be empty"):
        client.prepare("")


def test_prepared_statement_execute_no_params():
    """Execute a prepared SELECT without parameters."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'alice'), (2, 'bob')")

    stmt = client.prepare("SELECT id, name FROM mytable ORDER BY id")
    table = stmt.execute()
    stmt.close()

    assert table.num_rows == 2
    assert table.column("id")[0].as_py() == 1
    assert table.column("name")[1].as_py() == "bob"


def test_prepared_statement_execute_with_params():
    """Execute a prepared SELECT with bound parameters."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

    stmt = client.prepare("SELECT id, name FROM mytable WHERE id = ?")

    params = pa.record_batch(
        [pa.array([2], type=pa.uint32())],
        names=["param_1"],
    )
    stmt.bind_parameters(params)
    table = stmt.execute()

    assert table.num_rows == 1
    assert table.column("id")[0].as_py() == 2
    assert table.column("name")[0].as_py() == "bob"

    stmt.close()


def test_prepared_statement_execute_with_string_param():
    """Execute a prepared SELECT with a string parameter."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'alice'), (2, 'bob')")

    stmt = client.prepare("SELECT id FROM mytable WHERE name = ?")

    params = pa.record_batch(
        [pa.array(["alice"], type=pa.string())],
        names=["param_1"],
    )
    stmt.bind_parameters(params)
    table = stmt.execute()

    assert table.num_rows == 1
    assert table.column("id")[0].as_py() == 1

    stmt.close()


def test_prepared_statement_rebind_and_reexecute():
    """Rebind parameters and re-execute a prepared statement."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")
    client.execute_update("INSERT INTO mytable VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

    stmt = client.prepare("SELECT name FROM mytable WHERE id = ?")

    # First execution: id = 1
    params = pa.record_batch([pa.array([1], type=pa.uint32())], names=["p"])
    stmt.bind_parameters(params)
    table = stmt.execute()
    assert table.column("name")[0].as_py() == "alice"

    # Second execution: id = 3
    params = pa.record_batch([pa.array([3], type=pa.uint32())], names=["p"])
    stmt.bind_parameters(params)
    table = stmt.execute()
    assert table.column("name")[0].as_py() == "charlie"

    stmt.close()


def test_prepared_statement_update():
    """Execute a prepared INSERT via CommandPreparedStatementUpdate with rebinding."""
    client = get_client()

    client.execute_update("CREATE TABLE mytable (id UInt32, name String) ENGINE = Memory")

    stmt = client.prepare("INSERT INTO mytable VALUES (?, ?)")

    # First insert
    params = pa.record_batch(
        [pa.array([1], type=pa.uint32()), pa.array(["alice"], type=pa.string())],
        names=["p1", "p2"],
    )
    stmt.bind_parameters(params)
    stmt.execute_update()

    # Rebind and insert again
    params = pa.record_batch(
        [pa.array([2], type=pa.uint32()), pa.array(["bob"], type=pa.string())],
        names=["p1", "p2"],
    )
    stmt.bind_parameters(params)
    stmt.execute_update()

    # Third insert
    params = pa.record_batch(
        [pa.array([3], type=pa.uint32()), pa.array(["charlie"], type=pa.string())],
        names=["p1", "p2"],
    )
    stmt.bind_parameters(params)
    stmt.execute_update()

    stmt.close()

    # Verify all rows were inserted
    flight_info = client.execute("SELECT * FROM mytable ORDER BY id")
    reader = client.do_get(flight_info.endpoints[0].ticket)
    table = reader.read_all()

    assert table.num_rows == 3
    assert [table.column("id")[i].as_py() for i in range(3)] == [1, 2, 3]
    assert [table.column("name")[i].as_py() for i in range(3)] == ["alice", "bob", "charlie"]


#
# Transaction ID rejection tests
#

def test_transaction_id_rejected_for_statement_query():
    """CommandStatementQuery with transaction_id should be rejected."""
    client = get_client()
    cmd = CommandStatementQuery(query="SELECT 1", transaction_id=b"fake-txn-id")
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="transaction_id is not supported"):
        client.client.get_flight_info(flight_descriptor(cmd), client._flight_call_options())


def test_transaction_id_rejected_for_statement_update():
    """CommandStatementUpdate with transaction_id should be rejected."""
    client = get_client()
    cmd = CommandStatementUpdate(query="SELECT 1", transaction_id=b"fake-txn-id")
    desc = flight_descriptor(cmd)
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="transaction_id is not supported"):
        writer, reader = client.client.do_put(desc, pa.schema([]), client._flight_call_options())
        reader.read()
        writer.close()


def test_transaction_id_rejected_for_statement_ingest():
    """CommandStatementIngest with transaction_id should be rejected."""
    client = get_client()
    cmd = CommandStatementIngest(table="t", transaction_id=b"fake-txn-id")
    desc = flight_descriptor(cmd)
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="transaction_id is not supported"):
        writer, reader = client.client.do_put(desc, pa.schema([]), client._flight_call_options())
        reader.read()
        writer.close()


def test_transaction_id_rejected_for_create_prepared_statement():
    """CreatePreparedStatement with transaction_id should be rejected."""
    client = get_client()
    req = ActionCreatePreparedStatementRequest(query="SELECT 1", transaction_id=b"fake-txn-id")
    action = flight.Action("CreatePreparedStatement", req.SerializeToString())
    with pytest.raises(pa.lib.ArrowNotImplementedError, match="transaction_id is not supported"):
        list(client.client.do_action(action, client._flight_call_options()))
