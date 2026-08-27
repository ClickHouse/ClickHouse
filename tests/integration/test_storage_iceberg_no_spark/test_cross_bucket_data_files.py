#!/usr/bin/env python3

import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import list_s3_objects

BASE_URL = "http://rest:8181/v1"

CATALOG_NAME = "demo"

METADATA_BUCKET = "warehouse-rest"
DATA_BUCKET = "iceberg-data"


def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def create_clickhouse_iceberg_database(node, name):
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS catalog_type='rest', warehouse='demo', storage_endpoint='http://minio1:9001/{METADATA_BUCKET}'
    """
    )


def test_data_files_in_another_bucket(started_cluster_iceberg_no_spark, tmp_path):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog.create_namespace(root_namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    table = catalog.create_table(
        identifier=f"{root_namespace}.test_cross_bucket",
        schema=schema,
        location=f"s3://{METADATA_BUCKET}/cross_bucket/{root_namespace}",
    )

    # The data file lives in a bucket of its own, while the table metadata stays in the bucket
    # ClickHouse is configured with.
    df = pa.Table.from_pylist(
        [{"id": i, "name": f"row{i}"} for i in range(100)], schema=schema.as_arrow()
    )
    local_file = os.path.join(tmp_path, "data.parquet")
    pq.write_table(df, local_file)

    data_key = f"cross_bucket/{root_namespace}/data/data.parquet"
    started_cluster_iceberg_no_spark.minio_client.fput_object(
        DATA_BUCKET, data_key, local_file
    )
    table.add_files([f"s3://{DATA_BUCKET}/{data_key}"])

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)

    table_expression = f"{CATALOG_NAME}.`{root_namespace}.test_cross_bucket`"

    assert instance.query(f"SELECT count() FROM {table_expression}").strip() == "100"
    assert instance.query(f"SELECT sum(id) FROM {table_expression}").strip() == str(
        sum(range(100))
    )
    assert (
        instance.query(
            f"SELECT name FROM {table_expression} ORDER BY id LIMIT 1"
        ).strip()
        == "row0"
    )
    assert DATA_BUCKET in instance.query(
        f"SELECT DISTINCT _path FROM {table_expression}"
    )


def test_insert_writes_data_and_metadata_to_their_own_buckets(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog.create_namespace(root_namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    prefix = f"insert_split/{root_namespace}"
    catalog.create_table(
        identifier=f"{root_namespace}.test_insert_split",
        schema=schema,
        location=f"s3://{METADATA_BUCKET}/{prefix}",
        properties={"write.data.path": f"s3://{DATA_BUCKET}/{prefix}"},
    )

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)

    table_expression = f"{CATALOG_NAME}.`{root_namespace}.test_insert_split`"
    insert_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }

    instance.query(
        f"INSERT INTO {table_expression} VALUES (1, 'a'), (2, 'b')",
        settings=insert_settings,
    )
    instance.query(
        f"INSERT INTO {table_expression} VALUES (3, 'c')",
        settings=insert_settings,
    )

    minio = started_cluster_iceberg_no_spark.minio_client
    data_keys = list_s3_objects(minio, DATA_BUCKET, prefix=f"{prefix}/")
    metadata_keys = list_s3_objects(minio, METADATA_BUCKET, prefix=f"{prefix}/")

    assert len(data_keys) == 2, data_keys
    assert all(key.endswith(".parquet") for key in data_keys), data_keys

    assert not any(key.endswith(".parquet") for key in metadata_keys), metadata_keys
    assert any(key.endswith(".avro") for key in metadata_keys), metadata_keys
    assert any(key.endswith(".metadata.json") for key in metadata_keys), metadata_keys

    assert instance.query(f"SELECT count() FROM {table_expression}").strip() == "3"
    assert instance.query(f"SELECT sum(id) FROM {table_expression}").strip() == "6"
    assert (
        instance.query(
            f"SELECT name FROM {table_expression} ORDER BY id"
        ).strip()
        == "a\nb\nc"
    )
