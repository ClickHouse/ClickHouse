#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from datetime import date, timedelta
from pyiceberg.schema import Schema, NestedField
import random
from pyiceberg.types import (
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

BASE_URL = "http://rest:8181/v1"
BASE_URL_LOCAL = "http://localhost:8182/v1"
BASE_URL_LOCAL_RAW = "http://localhost:8182"

CATALOG_NAME = "demo"

def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": BASE_URL_LOCAL_RAW,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )

def create_table(
    catalog,
    namespace,
    table,
    schema,
    partition_spec,
    sort_order
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )

def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def test_sort_order(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(
            field_id=1, name="boolean_col", field_type=BooleanType(), required=False
        ),
        NestedField(field_id=2, name="long_col", field_type=LongType(), required=False),
        NestedField(
            field_id=3, name="double_col", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=4, name="string_col", field_type=StringType(), required=False
        ),
        NestedField(field_id=5, name="date_col", field_type=DateType(), required=False),
    )

    partition_spec = PartitionSpec()

    # NOTE pyiceberg ignores sort order when writing data, so writes data unsorted
    sort_order = SortOrder(SortField(source_id=4, transform=IdentityTransform()))
    table = create_table(catalog, root_namespace, "test", schema, partition_spec, sort_order)

    data = []
    for _ in range(100):
        data.append(
            {
                "boolean_col": random.choice([True, False]),
                "long_col": random.randint(1000, 10000),
                "double_col": round(random.uniform(1.0, 500.0), 2),
                "string_col": f"User{random.randint(1, 1000)}",
                "date_col": date.today()
                - timedelta(days=random.randint(0, 3650)),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    print(instance.query(f"SHOW TABLES FROM {CATALOG_NAME};"))

    # NOTE Read in order optimization shouldn't work because data is not sorted
    result = instance.query(f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=0").strip().split("\n")
    assert result == list(sorted(result))
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=0"
        )
    )

    result = instance.query(f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=1").strip().split("\n")
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=1"
        )
    )

    assert result == list(sorted(result))
