# coding: utf-8

import os
import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string
from .flight_sql_client import (
    FlightSQLClient,
    flight_descriptor,
    CommandStatementUpdate,
    DoPutUpdateResult,
    CancelStatus,
    SetSessionOptionsResult,
    CommandStatementQuery,
    CommandStatementIngest,
)


from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.test_tools import TSV


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

def get_client():
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        metadata={'x-clickhouse-session-id': session_id},
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
        "CREATE TABLE mytable (id UInt32, name String, value Float64) ENGINE = Memory"
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
    # table_schema column should contain serialized Arrow schema bytes
    schema_bytes = table.column("table_schema")[0].as_py()
    assert len(schema_bytes) > 0


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
