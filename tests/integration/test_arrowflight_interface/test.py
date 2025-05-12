# coding: utf-8

import os
import pytest
import pyarrow as pa
import pyarrow.flight as flight

from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_doput():
    node.query(
        """
        CREATE TABLE doput_test (
            id Int64,
            name String
        )
        ORDER BY id
        """
    )
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
    ], schema=schema)
    descriptor = flight.FlightDescriptor.for_path("doput_test")
    writer, _ = client.do_put(descriptor, schema)
    writer.write_batch(batch)
    writer.close()

    result = node.query(f"SELECT * FROM doput_test;")
    assert TSV(result) == TSV('1\tAlice\n2\tBob\n3\tCharlie\n')

    node.query("DROP TABLE IF EXISTS doput_test SYNC")


def test_doget():
    node.query(
        """
        CREATE TABLE doget_test (
            id Int64,
            name String
        )
        ORDER BY id
        """
    )
    node.query(
        """
            INSERT INTO doget_test VALUES(10, 'abc'), (20, 'cde')
        """)
    
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    real_ticket = flight.Ticket(b"SELECT * FROM doget_test")
    reader = client.do_get(real_ticket)
    actual = reader.read_all()

    expected_schema = pa.schema([
        pa.field("id",   pa.int64(),  nullable=False),
        pa.field("name", pa.string(), nullable=False),
    ])
    expected_ids   = pa.chunked_array([pa.array([10, 20],          type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"],    type=pa.string())])

    assert actual.schema == expected_schema

    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)

    node.query("DROP TABLE IF EXISTS doget_test SYNC")
