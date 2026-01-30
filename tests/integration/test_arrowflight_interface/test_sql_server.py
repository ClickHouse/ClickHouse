# coding: utf-8

import os
import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string
from flightsql import FlightSQLClient, FlightSQLCallOptions, flightsql_pb2
import flightsql


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
    
    This implementation uses a mix of the underlying Flight API with undocumented
    flightsql-dbapi functionality. When upgrading to Arrow Flight SQL v12+, this
    test should be replaced with the standard CommandStatementIngest approach.
    """
    client = get_client()

    client.execute_update("CREATE TABLE bulk_test (id UInt32, str String) ENGINE = Memory")

    cmd = flightsql_pb2.CommandStatementUpdate(query="INSERT INTO bulk_test FORMAT Arrow")
    descriptor = flightsql.client.flight_descriptor(cmd)
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
    update_result = flightsql_pb2.DoPutUpdateResult()
    update_result.ParseFromString(result.to_pybytes())
    assert update_result.record_count == 7000
