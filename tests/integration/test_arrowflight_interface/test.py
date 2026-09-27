# coding: utf-8

import os
import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string

from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

# Overrides the hostname checked by TLS, see certs/server-ext.cnf
SSL_HOST = "integration-tests.clickhouse.com"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/enable_ssl.xml",
        "configs/named_collection.xml",
        "certs/server-cert.pem",
        "certs/server-key.pem",
        "certs/ca-cert.pem",
    ],
    user_configs=["configs/users.xml"],
)


def get_client():
    with open(os.path.join(SCRIPT_DIR, "certs/ca-cert.pem"), "rb") as file:
        tls_root_certs = file.read()
    client = flight.FlightClient(
        f"grpc+tls://{node.ip_address}:8888",
        tls_root_certs=tls_root_certs,
        override_hostname=SSL_HOST,
    )
    token = client.authenticate_basic_token("user1", "qwe123")
    options = flight.FlightCallOptions(headers=[token])
    return client, options


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
        node.query("DROP TABLE IF EXISTS mytable SYNC")


def test_doput():
    node.query(
        """
        CREATE TABLE mytable (
            id Int64,
            name String
        )
        ORDER BY id
        """
    )
    client, options = get_client()
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )
    descriptor = flight.FlightDescriptor.for_path("mytable")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)
    writer.close()

    result = node.query("SELECT * FROM mytable")
    assert result == TSV([[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']])


def test_doput_nonexisting_table():
    client, options = get_client()
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_path("nonexisting_table")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)

    try:
        writer.close()
        assert False, "Expected error but query succeeded: insert into nonexisting table"
    except flight.FlightServerError as e:
        assert "Table default.nonexisting_table does not exist" in str(e)


def test_doput_cmd_insert():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
            name String
        ) ORDER BY id
    """
    )

    client, options = get_client()

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_command("INSERT INTO mytable FORMAT Arrow")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)
    writer.close()

    result = node.query("SELECT * FROM mytable ORDER BY id")
    assert result == TSV([[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']])


# INSERT queries with any format different from "Arrow" cannot be executed.
def test_doput_cmd_insert_invalid_format():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
            name String
        ) ORDER BY id
    """
    )

    client, options = get_client()

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_command("INSERT INTO mytable FORMAT JSON")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)

    try:
        writer.close()
        assert False, "Expected to fail because of a wrong format but succeeded"
    except flight.FlightServerError as e:
        assert "Invalid format, only 'Arrow' format is supported" in str(e)


# INSERT queries without the FORMAT clause are considered invalid.
def test_doput_cmd_insert_no_format():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
            name String
        ) ORDER BY id
    """
    )

    client, options = get_client()

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_command("INSERT INTO mytable")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)

    try:
        writer.close()
        assert False, "Expected to fail because of no format but succeeded"
    except flight.FlightServerError as e:
        assert "Syntax error" in str(e)


# INSERT SELECT queries can be executed via method doput.
def test_doput_cmd_insert_select():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
            name String DEFAULT 'unknown'
        ) ORDER BY id
    """
    )

    client, options = get_client()

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    # While inserting we set only column 'id', and column 'name' is set by default to 'unknown'.
    descriptor = flight.FlightDescriptor.for_command(
        "INSERT INTO mytable (id) SELECT number FROM numbers(3)"
    )
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)
    writer.close()

    result = node.query("SELECT * FROM mytable ORDER BY id")
    assert result == TSV([[0, 'unknown'], [1, 'unknown'], [2, 'unknown']])


# It's allowed to read only some columns from arrow table while inserting.
def test_doput_cmd_insert_from_smaller_schema():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
            name String DEFAULT 'unknown'
        ) ORDER BY id
    """
    )

    client, options = get_client()

    # This arrow schema doesn't contain field 'name', so the 'name' column will be set by default to 'unknown'.
    schema = pa.schema(
        [
            ("id", pa.int64()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([10, 20, 30], type=pa.int64()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_command(
        "INSERT INTO mytable (id) FORMAT Arrow"
    )
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)
    writer.close()

    result = node.query("SELECT * FROM mytable ORDER BY id")
    assert result == TSV([[10, 'unknown'], [20, 'unknown'], [30, 'unknown']])


# It's allowed to read only some columns from arrow table while inserting.
def test_doput_cmd_insert_from_bigger_schema():
    node.query(
        """
        CREATE TABLE mytable (
            id   Int64,
        ) ORDER BY id
    """
    )

    client, options = get_client()

    # This arrow schema doesn't contain field 'name', so the 'name' column will be set by default to 'unknown'.
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        ],
        schema=schema,
    )

    descriptor = flight.FlightDescriptor.for_command(
        "INSERT INTO mytable (id) FORMAT Arrow"
    )
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_batch(batch)
    writer.close()

    result = node.query("SELECT * FROM mytable ORDER BY id")
    assert result == TSV([1, 2, 3])


# Method doput allows SELECT queries, but doesn't return any results (i.e. it works like SELECT ... FORMAT NULL).
def test_doput_cmd_select():
    client, options = get_client()

    name = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(4)
    )

    query = f"SELECT '{name}'"
    escaped_query = query.replace("'", "\\'")

    descriptor = flight.FlightDescriptor.for_command(query)
    writer, _ = client.do_put(descriptor, pa.schema([]), options)
    writer.close()

    node.query("SYSTEM FLUSH LOGS query_log")

    print(
        node.query(
            f"""
        SELECT * FROM system.query_log WHERE query = '{escaped_query}'
        """
        )
    )

    assert 2 == int(
        node.query(
            f"""
        SELECT count() FROM system.query_log WHERE query = '{escaped_query}'
        """
        )
    )


# Invalid queries are handled too.
def test_doput_invalid_query():
    client, options = get_client()
    descriptor = flight.FlightDescriptor.for_command("BAD QUERY")
    writer, _ = client.do_put(descriptor, pa.schema([]), options)
    try:
        writer.close()
        assert False, "Expected error but query succeeded"
    except flight.FlightServerError as e:
        assert "Syntax error: failed at position 1 (BAD): BAD QUERY" in str(e)


def test_doget():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_path("mytable")
    flight_info = client.get_flight_info(descriptor, options)
    assert len(flight_info.endpoints) == 1
    ticket = flight_info.endpoints[0].ticket

    reader = client.do_get(ticket, options)
    actual = reader.read_all()

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    expected_ids = pa.chunked_array([pa.array([10, 20], type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"], type=pa.string())])

    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)


def test_doget_nonexisting_table():
    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_path("nonexisting_table")

    try:
        client.get_flight_info(descriptor, options)
        assert False, "Expected error but query succeeded: select from nonexisting table"
    except flight.FlightServerError as e:
        assert "Unknown table expression identifier 'nonexisting_table'" in str(e)


def test_doget_cmd_select():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM mytable")
    flight_info = client.get_flight_info(descriptor, options)
    assert len(flight_info.endpoints) == 1
    ticket = flight_info.endpoints[0].ticket

    reader = client.do_get(ticket, options)
    actual = reader.read_all()

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    expected_ids = pa.chunked_array([pa.array([10, 20], type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"], type=pa.string())])

    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)


# Tickets can be either returned by method get_flight_info or built directly from a query,
# see flight.Ticket(b"SELECT * FROM mytable") in this test.
def test_doget_ticket_from_select():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    ticket = flight.Ticket(b"SELECT * FROM mytable")
    reader = client.do_get(ticket, options)
    actual = reader.read_all()

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    expected_ids = pa.chunked_array([pa.array([10, 20], type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"], type=pa.string())])

    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)


# If a query outputs multiple chunks the method get_flight_info returns multiple tickets.
def test_doget_multiple_chunks():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("SYSTEM STOP MERGES mytable")
    node.query("INSERT INTO mytable VALUES (10, 'abc')")
    node.query("INSERT INTO mytable VALUES (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM mytable ORDER BY id")
    flight_info = client.get_flight_info(descriptor, options)
    assert len(flight_info.endpoints) == 2
    tickets = [endpoint.ticket for endpoint in flight_info.endpoints]

    readers = [client.do_get(ticket, options) for ticket in tickets]
    actual = [reader.read_all() for reader in readers]

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )

    assert actual[0].schema == expected_schema
    assert actual[1].schema == expected_schema
    assert actual[0].column("id").equals(pa.chunked_array([pa.array([10], type=pa.int64())]))
    assert actual[1].column("id").equals(pa.chunked_array([pa.array([20], type=pa.int64())]))
    assert actual[0].column("name").equals(pa.chunked_array([pa.array(["abc"], type=pa.string())]))
    assert actual[1].column("name").equals(pa.chunked_array([pa.array(["cde"], type=pa.string())]))


# We don't support any formats different from "Arrow".
def test_doget_cmd_select_invalid_format():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM mytable FORMAT JSON")
    try:
        client.get_flight_info(descriptor, options)
        assert False, "Expected to fail because of a wrong format but succeeded"
    except flight.FlightServerError as e:
        assert "Invalid format, only 'Arrow' format is supported" in str(e)

    ticket = flight.Ticket(b"SELECT * FROM mytable FORMAT JSON")
    try:
        reader = client.do_get(ticket, options)
        assert False, "Expected to fail because of a wrong format but succeeded"
    except flight.FlightServerError as e:
        assert "Invalid format, only 'Arrow' format is supported" in str(e)


def test_doget_cmd_select_arrow_format():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM mytable FORMAT Arrow")
    flight_info = client.get_flight_info(descriptor, options)
    assert len(flight_info.endpoints) == 1
    ticket = flight_info.endpoints[0].ticket

    reader = client.do_get(ticket, options)
    actual = reader.read_all()

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    expected_ids = pa.chunked_array([pa.array([10, 20], type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"], type=pa.string())])

    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)

    ticket = flight.Ticket(b"SELECT * FROM mytable")
    reader = client.do_get(ticket, options)
    actual = reader.read_all()

    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)


# Invalid queries are handled too.
def test_doget_invalid_query():
    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("BAD QUERY")
    try:
        client.get_flight_info(descriptor, options)
        assert False, "Expected error but query succeeded"
    except flight.FlightServerError as e:
        assert "Syntax error: failed at position 1 (BAD): BAD QUERY" in str(e)

    ticket = flight.Ticket(b"BAD QUERY")
    try:
        client.do_get(ticket, options)
        assert False, "Expected error but query succeeded"
    except flight.FlightServerError as e:
        assert "Syntax error: failed at position 1 (BAD): BAD QUERY" in str(e)


# Method doget doesn't work with INSERT queries.
def test_doget_invalid_query_insert():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("INSERT INTO mytable FORMAT Arrow")
    try:
        client.get_flight_info(descriptor, options)
        assert False, "Expected error but query succeeded"
    except pa.lib.ArrowInvalid as e:
        assert "Query doesn't allow pulling data, use method doPut() with this kind of query" in str(e)

    ticket = flight.Ticket(b"INSERT INTO mytable FORMAT Arrow")
    try:
        client.do_get(ticket, options)
        assert False, "Expected error but query succeeded"
    except pa.lib.ArrowInvalid as e:
        assert "Query doesn't allow pulling data, use method doPut() with this kind of query" in str(e)


def test_invalid_user():
    client = flight.FlightClient(
        f"grpc+tls://{node.ip_address}:8888", disable_server_verification=True
    )
    token = client.authenticate_basic_token(b"invalid", b"password")
    options = flight.FlightCallOptions(headers=[token])
    ticket = flight.Ticket(b"SELECT * FROM mytable")
    try:
        client.do_get(ticket, options)
        assert False, "Expected authentication failure (login and password are not correct) but succeeded"
    except flight.FlightServerError as e:
        assert (
            "Authentication failed: password is incorrect, or there is no user with such name"
            in str(e)
        )


def test_table_function():
    node.query(
        """
        CREATE TABLE mytable (
            id Int64,
            name String
        )
        ENGINE=MergeTree
        ORDER BY id
        """
    )

    node.query("INSERT INTO mytable VALUES (1, 'a'), (2, 'b')")

    assert node.query(
        "SELECT * FROM arrowFlight(flight1, dataset = 'mytable') ORDER BY id"
    ) == TSV([[1, "a"], [2, "b"]])

    node.query(
        "INSERT INTO FUNCTION arrowFlight(flight1, dataset = 'mytable') VALUES (3, 'c')"
    )

    assert node.query(
        "SELECT * FROM arrowFlight(flight1, dataset = 'mytable') ORDER BY id"
    ) == TSV([[1, "a"], [2, "b"], [3, "c"]])
