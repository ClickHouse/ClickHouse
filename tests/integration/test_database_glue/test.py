#!/usr/bin/env python3
import glob
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pyarrow as pa
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

import boto3

CATALOG_NAME = "test"

BASE_URL = "http://glue:3000"
BASE_URL_LOCAL_HOST = "http://localhost:3000"

DEFAULT_SCHEMA = Schema(
    NestedField(
        field_id=1, name="datetime", field_type=TimestampType(), required=False
    ),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=3, name="bid", field_type=DoubleType(), required=False),
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

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\\n(\\n    `datetime` Nullable(DateTime64(6)),\\n    `symbol` Nullable(String),\\n    `bid` Nullable(Float64),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))


def list_databases():
    client = boto3.client("glue", region_name="us-east-1", endpoint_url=BASE_URL_LOCAL_HOST)
    databases = client.get_databases()
    return databases


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME, # name is not important
        **{
            "type": "glue",
            "glue.endpoint": BASE_URL_LOCAL_HOST,
            "glue.region": "us-east-1",
            "s3.endpoint": "http://localhost:9002",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
        },)

def create_table(
    catalog,
    namespace,
    table,
    schema=DEFAULT_SCHEMA,
    partition_spec=DEFAULT_PARTITION_SPEC,
    sort_order=DEFAULT_SORT_ORDER,
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )


def generate_record():
    return {
        "datetime": datetime.now(),
        "symbol": str("kek"),
        "bid": round(random.uniform(100, 200), 2),
        "ask": round(random.uniform(200, 300), 2),
        "details": {"created_by": "Alice Smith"},
    }


def create_clickhouse_glue_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "glue",
        "warehouse": "test",
        "storage_endpoint": "http://minio:9000/warehouse",
        "region": "us-east-1",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_experimental_database_glue_catalog=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', 'minio123')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )


def print_objects():
    minio_client = Minio(
        f"minio:9002",
        access_key="minio",
        secret_key="minio123",
        secure=False,
        http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
    )

    objects = list(minio_client.list_objects("warehouse", "", recursive=True))
    names = [x.object_name for x in objects]
    names.sort()
    for name in names:
        print(f"Found object: {name}")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        # We must add some credentials, otherwise moto (AWS Mock)
        # will reject boto connection
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_glue_catalog=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_list_tables(started_cluster):
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}_testA_A"
    namespace_2 = f"{root_namespace}_testB_B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster)

    for namespace in [namespace_1, namespace_2]:
        catalog.create_namespace(namespace)

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables(namespace)) == 0

    create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

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
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )
    node.restart_clickhouse()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )

    expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace_2, "tableC")
    print("Expected", expected)
    print("Got", node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"
    ))
    assert expected == node.query(
        f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace_2}.tableC`"
    )

def test_select(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}_A",
        f"{root_namespace}_B",
        f"{root_namespace}_C",
    ]

    catalog = load_catalog_impl(started_cluster)

    for namespace in namespaces_to_create:
        catalog.create_namespace(namespace)
        assert len(catalog.list_tables(namespace)) == 0

    for namespace in namespaces_to_create:
        table = create_table(catalog, namespace, table_name)

        num_rows = 10
        data = [generate_record() for _ in range(num_rows)]
        df = pa.Table.from_pylist(data)
        table.append(df)

        create_clickhouse_glue_database(started_cluster, node, CATALOG_NAME)

        expected = DEFAULT_CREATE_TABLE.format(CATALOG_NAME, namespace, table_name)
        assert expected == node.query(
            f"SHOW CREATE TABLE {CATALOG_NAME}.`{namespace}.{table_name}`"
        )

        assert num_rows == int(
            node.query(f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}`")
        )


def test_hide_sensitive_info(started_cluster):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}_A"
    catalog = load_catalog_impl(started_cluster)
    catalog.create_namespace(namespace)

    table = create_table(catalog, namespace, table_name)

    create_clickhouse_glue_database(
        started_cluster,
        node,
        CATALOG_NAME,
        additional_settings={"aws_access_key_id": "SECRET_1", "aws_secret_access_key": "SECRET_2"},
    )
    assert "SECRET_1" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
    assert "SECRET_2" not in node.query(f"SHOW CREATE DATABASE {CATALOG_NAME}")
