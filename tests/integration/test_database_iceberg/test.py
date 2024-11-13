import glob
import json
import logging
import os
import time
import uuid

import pytest
import requests
import urllib3
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    FloatType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket
from helpers.test_tools import TSV, csv_compare

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8182/v1"
BASE_URL_LOCAL_RAW = "http://localhost:8182"

CATALOG_NAME = "demo"

DEFAULT_SCHEMA = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
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
DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)
DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


def list_namespaces():
    response = requests.get(f"{BASE_URL_LOCAL}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")


def load_catalog_impl():
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": BASE_URL_LOCAL_RAW,
            "type": "rest",
            "s3.endpoint": f"http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
        },
    )


def create_table(
    catalog,
    namespace,
    table,
    schema=DEFAULT_SCHEMA,
    partition_spec=DEFAULT_PARTITION_SPEC,
    sort_order=DEFAULT_SORT_ORDER,
):
    catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )


def create_clickhouse_iceberg_database(started_cluster, node, name):
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
CREATE DATABASE {name} ENGINE = Iceberg('{BASE_URL}', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/'
    """
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

        # TODO: properly wait for container
        time.sleep(10)
        #cluster.minio_client.make_bucket("warehouse")

        yield cluster

    finally:
        cluster.shutdown()


def test_simple(started_cluster):
    node = started_cluster.instances["node1"]

    root_namespace = "clickhouse"
    namespace_1 = "clickhouse.testA.A"
    namespace_2 = "clickhouse.testB.B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl()

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

    assert root_namespace in list_namespaces()["namespaces"][0][0]
    assert [(root_namespace,)] == catalog.list_namespaces()

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables(namespace)) == 0

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    tables_list = ""
    for table in namespace_1_tables:
        create_table(catalog, namespace_1, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_1}.{table}"

    for table in namespace_2_tables:
        create_table(catalog, namespace_2, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_2}.{table}"

    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' ORDER BY name"
        ).strip()
    )
    node.restart_clickhouse()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' ORDER BY name"
        ).strip()
    )

    expected = f"CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`\\n(\\n    `datetime` DateTime64(6),\\n    `symbol` String,\\n    `bid` Nullable(Float32),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://None:9001/warehouse\\', \\'minio\\', \\'[HIDDEN]\\')\n"
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"
    )


def test_different_namespaces(started_cluster):
    node = started_cluster.instances["node1"]
    namespaces = [
        "A",
        "A.B.C",
        "A.B.C.D",
        "A.B.C.D.E",
        "A.B.C.D.E.F",
        "A.B.C.D.E.FF",
        "B",
        "B.C",
        "B.CC",
    ]
    tables = ["A", "B", "C", "D", "E", "F"]
    catalog = load_catalog_impl()

    for namespace in namespaces:
        # if namespace in catalog.list_namespaces()["namesoaces"]:
        #    catalog.drop_namespace(namespace)
        catalog.create_namespace(namespace)
        for table in tables:
            create_table(catalog, namespace, table)

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    for namespace in namespaces:
        for table in tables:
            table_name = f"{namespace}.{table}"
            assert int(
                node.query(
                    f"SELECT count() FROM system.tables WHERE database = '{CATALOG_NAME}' and name = '{table_name}'"
                )
            )
