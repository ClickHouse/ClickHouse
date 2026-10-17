#!/usr/bin/env python3
import pytest
import time
import os
import uuid
import socket

import pyarrow as pa
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


def wait_for_hms(started_cluster, timeout=120):
    """Wait until the Hive Metastore TCP port 9083 is accepting connections."""
    hive_ip = started_cluster.get_instance_ip("hive")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((hive_ip, 9083), timeout=2):
                return
        except OSError:
            time.sleep(1)
    raise Exception(f"Hive Metastore at {hive_ip}:9083 did not become available within {timeout}s")


def load_hive_catalog(started_cluster):
    return load_catalog(
        "hive",
        **{
            "uri": f"thrift://{started_cluster.get_instance_ip('hive')}:9083",
            "type": "hive",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
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

    # Catalog initialization now happens in the DatabaseDataLake constructor, so
    # CREATE DATABASE connects to the Hive Metastore eagerly. Make sure the
    # metastore is accepting connections before issuing it, otherwise the
    # statement fails with a TTransportException during construction.
    wait_for_hms(started_cluster)

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

    wait_for_hms(started_cluster)
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
    # Give the old HMS process time to stop before probing the new one.
    time.sleep(2)
    wait_for_hms(started_cluster)

    tables_after = get_tables_from_clickhouse(node, "lake_test")
    assert sorted(tables_before) == sorted(tables_after), (
        f"Tables list changed after restart. Before: {sorted(tables_before)}, After: {sorted(tables_after)}"
    )

    node.query("DROP DATABASE IF EXISTS lake_test")


def test_recreated_table_with_compaction_enabled(started_cluster):
    """
    A table dropped and recreated in the Hive Metastore under the same name and location, with another schema, is read
    as the new table, also through a database with `allow_experimental_iceberg_compaction`, which keeps its tables.
    https://github.com/ClickHouse/ClickHouse/issues/122547
    """
    node = started_cluster.instances["node1"]
    namespace = f"test_recreated_table_{uuid.uuid4().hex[:8]}"
    identifier = f"{namespace}.table"
    location = f"s3a://warehouse-hms/data/{namespace}/table"
    databases = [f"{namespace}_plain", f"{namespace}_compaction"]

    wait_for_hms(started_cluster)
    catalog = load_hive_catalog(started_cluster)
    catalog.create_namespace(namespace)
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "a", LongType())), location=location)
    table.append(pa.table({"a": pa.array([1, 2], type=pa.int64())}))

    for db, extra_settings in zip(databases, ["", ", allow_experimental_iceberg_compaction = 1"]):
        node.query(f"""
            CREATE DATABASE {db} ENGINE = DataLakeCatalog('thrift://hive:9083', '{minio_access_key}', '{minio_secret_key}')
            SETTINGS catalog_type = 'hive', warehouse = 'warehouse_test',
                     storage_endpoint = 'http://minio1:9001/warehouse-hms'{extra_settings}
        """)

    def check(expected):
        for db in databases:
            assert node.query(f"SELECT * FROM {db}.`{identifier}` ORDER BY ALL") == expected, db

    check("1\n2\n")

    # An unchanged read opens a storage through the plain database only.
    def opened_storages(db):
        query_id = uuid.uuid4().hex
        node.query(f"SELECT * FROM {db}.`{identifier}` FORMAT Null", query_id=query_id)
        node.query("SYSTEM FLUSH LOGS query_log")
        return node.query(
            f"SELECT length(used_storages) FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )

    assert [opened_storages(db) for db in databases] == ["1\n", "0\n"]

    catalog.drop_table(identifier)
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "b", StringType())), location=location)
    table.append(pa.table({"b": pa.array(["x", "y"], type=pa.string())}))
    check("x\ny\n")

    for db in databases:
        node.query(f"DROP DATABASE {db}")
