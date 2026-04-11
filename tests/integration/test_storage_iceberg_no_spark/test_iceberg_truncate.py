#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType
from pyiceberg.partitioning import PartitionSpec

CATALOG_NAME = "demo"

def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def test_iceberg_truncate_restart(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    namespace = f"clickhouse_truncate_restart_{uuid.uuid4().hex}"
    catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="val", field_type=StringType(), required=False),
    )
    table_name = "test_truncate_restart"
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location=f"s3://warehouse-rest/{namespace}.{table_name}",
        partition_spec=PartitionSpec(),
    )

    ch_table_identifier = f"`{namespace}.{table_name}`"

    instance.query(f"DROP DATABASE IF EXISTS {namespace}")
    instance.query(
        f"""
        CREATE DATABASE {namespace} ENGINE = DataLakeCatalog('http://rest:8181/v1', 'minio', '{minio_secret_key}')
        SETTINGS
            catalog_type='rest',
            warehouse='demo',
            storage_endpoint='http://minio:9000/warehouse-rest';
        """,
        settings={"allow_database_iceberg": 1}
    )

    # 1. Insert initial data and truncate
    df = pa.Table.from_pylist([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}])
    catalog.load_table(f"{namespace}.{table_name}").append(df)

    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 2

    instance.query(
        f"TRUNCATE TABLE {namespace}.{ch_table_identifier}",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 0

    # 2. Restart ClickHouse and verify table is still readable (count = 0)
    instance.restart_clickhouse()
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 0

    # 3. Insert new data after restart and verify it's readable
    new_df = pa.Table.from_pylist([{"id": 3, "val": "C"}])
    catalog.load_table(f"{namespace}.{table_name}").append(new_df)
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 1

    instance.query(f"DROP DATABASE {namespace}")