import os
import time
import uuid
from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType
import requests
from helpers.cluster import ClickHouseCluster
from google.cloud import storage
from helpers.mock_servers import start_mock_servers


CATALOG_NAME = "demo"
WAREHOUSE_LOCATION = "gs://warehouse"

GCS_PROJECT = "test-project"
GCS_ENDPOINT_HOST = "http://localhost:4443"
GCS_ENDPOINT_DOCKER = "http://gcs:4443"


import pyspark
from pyspark.sql import SparkSession

METADATA_SERVER_HOSTNAME = "resolver"
METADATA_SERVER_PORT = 8182

BASE_URL_DOCKER = f"http://{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}/v1"
BASE_URL_HOST = "http://localhost:8182"

def start_rest_gcs_server(cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "rest_with_gcs")
    start_mock_servers(
        cluster,
        script_dir,
        [
            (
                "mock.py",
                METADATA_SERVER_HOSTNAME,
                METADATA_SERVER_PORT,
            )
        ],
    )

@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node1",
        stay_alive=True,
        with_iceberg_gcs_catalog=True,
        env_variables={
            "AWS_EC2_METADATA_SERVICE_ENDPOINT": f"{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}",
        },
        main_configs=[
            "configs/use_environment_credentials.xml",
        ],

    )

    cluster.start()
    start_rest_gcs_server(cluster)
    time.sleep(5)
    yield cluster
    cluster.shutdown()


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "type": "rest",
            "uri": f"http://{started_cluster.get_instance_ip('resolver')}:{METADATA_SERVER_PORT}",
            f"gcs.service.host": f"http://{started_cluster.get_instance_ip('gcs')}:4443",
            f"gcs.endpoint": f"http://{started_cluster.get_instance_ip('gcs')}:4443",
            f"s3.endpoint" : f"http://{started_cluster.get_instance_ip('gcs')}:4443",
            "gcs.project-id": GCS_PROJECT,
        },
    )


def test_show_tables_and_select(started_cluster):
    os.environ["GOOGLE_CLOUD_PROJECT"] = GCS_PROJECT
    os.environ["STORAGE_EMULATOR_HOST"] = f"http://{started_cluster.get_instance_ip('gcs')}:4443"

    client = storage.Client(project="test-project")

    bucket_name = "warehouse"
    blob_name = "00000-aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa.metadata.json"

    bucket = client.bucket(bucket_name)
    bucket.create()
    blob = bucket.blob(blob_name)

    data = """
{
  "format-version": 2,
  "table-uuid": "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa",
  "location": "gs://warehouse/ns_test/t",
  "last-sequence-number": 0,
  "last-updated-ms": 1700000000000,
  "last-column-id": 2,

  "schemas": [
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {
          "id": 1,
          "name": "ts",
          "required": false,
          "type": "timestamptz"
        },
        {
          "id": 2,
          "name": "value",
          "required": false,
          "type": "string"
        }
      ]
    }
  ],

  "current-schema-id": 0,

  "partition-specs": [
    {
      "spec-id": 0,
      "fields": []
    }
  ],
  "default-spec-id": 0,

  "sort-orders": [
    {
      "order-id": 0,
      "fields": []
    }
  ],
  "default-sort-order-id": 0,

  "properties": {},

  "snapshots": [],
  "current-snapshot-id": null,
  "snapshot-log": [],
  "metadata-log": []
}
    """

    blob.upload_from_string(
        data,
        content_type="application/json",
    )

    node = started_cluster.instances["node1"]
    catalog = load_catalog_impl(started_cluster)

    namespace = f"ns_{uuid.uuid4().hex}"
    table = "t"

    catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(1, "ts", TimestampType(), required=False),
        NestedField(2, "value", StringType(), required=False),
    )

    object_name = (
        f"00000-aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa.metadata.json"
    )


    iceberg_table = catalog.register_table(
        (namespace, table),
        f"gs://warehouse/00000-aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa.metadata.json"
    )

    node.query(
        f"""
        DROP DATABASE IF EXISTS {CATALOG_NAME};
        SET allow_database_iceberg = 1;

        CREATE DATABASE {CATALOG_NAME}
        ENGINE = DataLakeCatalog('{BASE_URL_DOCKER}', 'gcs', 'dummy')
        SETTINGS
            catalog_type = 'rest',
            warehouse = 'demo',
            storage_endpoint = '{GCS_ENDPOINT_DOCKER}'
        """
    )

    tables = node.query(f"SHOW TABLES FROM {CATALOG_NAME}")
    assert f"{namespace}.{table}" in tables
