import io
import logging

import avro.schema
import pytest
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from helpers.cluster import ClickHouseCluster, ClickHouseInstance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("dummy", with_kafka=True)
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


def test_select(started_cluster):
    # type: (ClickHouseCluster) -> None

    schema_registry_client = CachedSchemaRegistryClient(
        "http://localhost:{}".format(started_cluster.schema_registry_port)
    )
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, 8081
    )

    run_query(instance, "create table avro_data(value Int64) engine = Memory()")
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(instance, "insert into avro_data format AvroConfluent", data, settings)
    stdout = run_query(instance, "select * from avro_data")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]
