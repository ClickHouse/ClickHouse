#!/usr/bin/env python3

import uuid

from helpers.config_cluster import minio_secret_key, minio_access_key
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder
from pyiceberg.transforms import IdentityTransform

BASE_URL = "http://rest:8181/v1"
CATALOG_NAME = "demo"


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


def create_clickhouse_iceberg_database(started_cluster, node, name):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in settings.items()))}
    """
    )


def test_drop_partition_catalog_backed(started_cluster_iceberg_no_spark):
    """DROP PARTITION on a catalog-backed (REST) table must commit the new metadata
    location to the catalog, so that an independent catalog reader (pyiceberg) also
    sees the dropped partition. Before the fix the executor only wrote the metadata
    file and never called catalog->updateMetadata, leaving the catalog stale."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    namespace = f"clickhouse_{uuid.uuid4()}"
    table_name = "drop_partition"

    schema = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="a")
    )
    catalog.create_namespace(namespace)
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location="s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=SortOrder(),
    )

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    qualified = f"{CATALOG_NAME}.`{namespace}.{table_name}`"

    instance.query(
        f"INSERT INTO {qualified} VALUES (1, 'x'), (2, 'y'), (3, 'z')",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {qualified}").strip() == "3"

    instance.query(
        f"ALTER TABLE {qualified} DROP PARTITION 2",
        settings={"allow_insert_into_iceberg": 1},
    )

    # ClickHouse sees the drop.
    assert instance.query(f"SELECT a FROM {qualified} ORDER BY a").strip() == "1\n3"

    # Re-create the database so the table's metadata location is re-read from the catalog. Before the
    # fix the DROP only wrote the metadata file and never called catalog->updateMetadata, so a fresh
    # catalog reader would still see the pre-drop metadata (3 rows).
    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    assert instance.query(f"SELECT count() FROM {qualified}").strip() == "2"
    assert instance.query(f"SELECT a FROM {qualified} ORDER BY a").strip() == "1\n3"
