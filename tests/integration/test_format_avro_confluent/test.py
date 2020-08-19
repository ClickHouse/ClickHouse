import json
import logging
import io

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

import helpers.client

import avro.schema
from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

@pytest.fixture(scope="module")
def cluster():
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



def test_select(cluster):
    # type: (ClickHouseCluster) -> None

    schema_registry_client = cluster.schema_registry_client
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object({
        'name': 'test_record',
        'type': 'record',
        'fields': [
            {
                'name': 'value',
                'type': 'long' 
            }
        ]
    })

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            'test_subject', schema, {'value': x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        cluster.schema_registry_host,
        cluster.schema_registry_port
    )
    
    run_query(instance, "create table avro_data(value Int64) engine = Memory()")
    settings = {'format_avro_schema_registry_url': schema_registry_url}
    run_query(instance, "insert into avro_data format AvroConfluent", data, settings)
    stdout = run_query(instance, "select * from avro_data")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]
