import io
import logging
import time
from urllib import parse
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .message_pb2 import TestRecord

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm

# Skip on ARM due to Confluent/Kafka
if is_arm():
    pytestmark = pytest.mark.skip


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("dummy", with_kafka=True, with_secrets=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, data=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    # use http to force parsing on server
    if not data:
        data = " "  # make POST request
    result = instance.http_query(query, data=data, params=settings)
    logging.info("Query finished")

    return result

def get_uuid_str():
    return str(uuid.uuid4()).replace("-", "_")


def test_select(started_cluster):
    reg_url = f"http://localhost:{started_cluster.schema_registry_port}"
    schema_registry_conf = {"url": reg_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    serializer = ProtobufSerializer(
        TestRecord,
        schema_registry_client,
        {"use.deprecated.format": False},
    )

    buf = io.BytesIO()
    ctx = SerializationContext(topic="test_subject", field=MessageField.VALUE)

    for x in range(42, 45):
        record = TestRecord(value=x, value2="abc")
        message = serializer(record, ctx)
        buf.write(message)

    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]
    schema_registry_url = f"http://{started_cluster.schema_registry_host}:{started_cluster.schema_registry_port}"

    uuid_table = get_uuid_str()
    run_query(instance, f"create table protobuf_data{uuid_table}(value Int64, value2 String) engine = Memory()")
    settings = {"format_protobuf_schema_registry_url": schema_registry_url}

    run_query(instance, f"insert into protobuf_data{uuid_table} format ProtobufConfluent", data, settings)
    stdout = run_query(instance, f"select * from protobuf_data{uuid_table}")
    assert list(map(str.split, stdout.splitlines())) == [['42', 'abc'], ['43', 'abc'], ['44', 'abc']]


def test_select_auth(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = f"http://localhost:{started_cluster.schema_registry_auth_port}"
    schema_registry_conf = {
        "url": reg_url,
        "basic.auth.user.info": "schemauser:letmein",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    serializer = ProtobufSerializer(
        TestRecord,
        schema_registry_client,
        {"use.deprecated.format": False},
    )

    buf = io.BytesIO()
    ctx = SerializationContext(topic="test_subject", field=MessageField.VALUE)

    for x in range(42, 45):
        record = TestRecord(value=x, value2="abc")
        message = serializer(record, ctx)
        buf.write(message)

    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        "schemauser",
        "letmein",
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    uuid_table = get_uuid_str()
    run_query(instance, f"create table protobuf_data{uuid_table}(value Int64, value2 String) engine = Memory()")
    settings = {"format_protobuf_schema_registry_url": schema_registry_url}

    run_query(instance, f"insert into protobuf_data{uuid_table} format ProtobufConfluent", data, settings)
    stdout = run_query(instance, f"select * from protobuf_data{uuid_table}")
    assert list(map(str.split, stdout.splitlines())) == [['42', 'abc'], ['43', 'abc'], ['44', 'abc']]
