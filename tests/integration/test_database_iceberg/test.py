import glob
import json
import logging
import os
import time
import uuid

import pytest
import requests
from minio import Minio
import urllib3

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform


from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8181/v1"
BASE_URL_LOCAL_RAW = "http://localhost:8181"


def create_namespace(name):
    payload = {
        "namespace": [name],
        "properties": {"owner": "clickhouse", "description": "test namespace"},
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{BASE_URL_LOCAL}/namespaces", headers=headers, data=json.dumps(payload)
    )
    if response.status_code == 200:
        print(f"Namespace '{name}' created successfully.")
    else:
        raise Exception(
            f"Failed to create namespace. Status code: {response.status_code}, Response: {response.text}"
        )


def list_namespaces():
    response = requests.get(f"{BASE_URL_LOCAL}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")


def create_table(name, namespace):
    payload = {
        "name": name,
        "location": "s3://warehouse/",
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "name", "type": "String", "required": True},
                {"id": 2, "name": "age", "type": "Int", "required": False},
            ],
        },
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{BASE_URL_LOCAL}/namespaces/{namespace}/tables",
        headers=headers,
        data=json.dumps(payload),
    )
    if response.status_code == 200:
        print(f"Table '{name}' created successfully.")
    else:
        raise Exception(
            f"Failed to create a table. Status code: {response.status_code}, Response: {response.text}"
        )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        # prepare_s3_bucket(cluster)

        yield cluster

    finally:
        cluster.shutdown()


def test_simple(started_cluster):
    # TODO: properly wait for container
    time.sleep(10)

    namespace_1 = "clickhouse.test.A"
    root_namespace = "clickhouse"

    create_namespace(namespace_1)
    assert root_namespace in list_namespaces()["namespaces"][0][0]

    node = started_cluster.instances["node1"]
    node.query(
        f"""
CREATE DATABASE demo ENGINE = Iceberg('{BASE_URL}', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/'
    """
    )

    catalog_name = "demo"

    catalog = load_catalog(
        "demo",
        **{
            "uri": BASE_URL_LOCAL_RAW,
            "type": "rest",
            "s3.endpoint": f"http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
        },
    )
    namespace_2 = "clickhouse.test.B"
    catalog.create_namespace(namespace_2)

    assert [(root_namespace,)] == catalog.list_namespaces()

    tables = catalog.list_tables(namespace_2)
    assert len(tables) == 0

    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=True
        ),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=5,
            name="details",
            field_type=StructType(
                NestedField(
                    field_id=4,
                    name="created_by",
                    field_type=StringType(),
                    required=False,
                ),
            ),
            required=False,
        ),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )

    sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

    for table in ["tableA", "tableB"]:
        catalog.create_table(
            identifier=f"{namespace_2}.{table}",
            schema=schema,
            location=f"s3://warehouse",
            partition_spec=partition_spec,
            sort_order=sort_order,
        )

    def check():
        assert f"{namespace_2}.tableA\n{namespace_2}.tableB\n" == node.query("SELECT name FROM system.tables WHERE database = 'demo' ORDER BY name")

        expected = "CREATE TABLE demo.`clickhouse.test.B.tableA`\\n(\\n    `datetime` DateTime64(6),\\n    `symbol` String,\\n    `bid` Nullable(Float32),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://None:9001/warehouse\\', \\'minio\\', \\'[HIDDEN]\\')\n";
        assert expected == node.query(f"SHOW CREATE TABLE demo.`{namespace_2}.tableA`")

    check()
    node.restart_clickhouse()
    check()
