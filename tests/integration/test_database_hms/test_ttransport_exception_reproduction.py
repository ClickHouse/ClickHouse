#!/usr/bin/env python3
import pytest
import time
import os
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType


# Default schema for test tables
DEFAULT_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="value", field_type=LongType(), required=False),
)


def load_hive_catalog(started_cluster):
    return load_catalog(
        "hive",
        **{
            "uri": f"thrift://{started_cluster.get_instance_ip('hive')}:9083",
            "type": "hive",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def get_tables_from_clickhouse(node, database_name):
    result = node.query(f"SHOW TABLES FROM {database_name}", ignore_error=True)
    if result.strip():
        return sorted([line.strip() for line in result.strip().split('\n')])
    return []


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["config.xml"],
            user_configs=["users.xml"],
            with_hms_catalog=True,
            stay_alive=True,
        )
        cluster.start()
        time.sleep(10)
        yield cluster
    finally:
        cluster.shutdown()


def test_ttransport_exception_restart_service(started_cluster):
    password = os.environ.get('MINIO_PASSWORD', '[HIDDEN]')

    node = started_cluster.instances["node1"]
    node.query(f"""
        CREATE DATABASE IF NOT EXISTS lake_test
        ENGINE = DataLakeCatalog('thrift://hive:9083', 'minio', '{password}')
        SETTINGS catalog_type = 'hive',
                 warehouse = 'warehouse_test',
                 storage_endpoint = 'http://minio:9000/warehouse-hms/data/'
    """)

    catalog = load_hive_catalog(started_cluster)
    namespace = f"test_namespace_{uuid.uuid4().hex[:8]}"
    table_names = [f"table_{i}" for i in range(3)]

    catalog.create_namespace(namespace)
    for table_name in table_names:
        catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=DEFAULT_SCHEMA,
            location=f"s3a://warehouse-hms/data/{namespace}/{table_name}",
        )

    tables_before = get_tables_from_clickhouse(node, "lake_test")
    expected_tables = [f"{namespace}.{table_name}" for table_name in table_names]

    assert all(table in tables_before for table in expected_tables), (
        f"Not all expected tables found. Expected: {expected_tables}, Got: {tables_before}"
    )

    started_cluster.restart_service("hive")
    time.sleep(10)

    tables_after = get_tables_from_clickhouse(node, "lake_test")
    assert sorted(tables_before) == sorted(tables_after), (
        f"Tables list changed after restart. Before: {sorted(tables_before)}, After: {sorted(tables_after)}"
    )

    node.query("DROP DATABASE IF EXISTS lake_test")
