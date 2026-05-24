#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from datetime import date, timedelta
from pyiceberg.schema import Schema, NestedField
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

CATALOG_NAME = "demo"

def load_catalog_impl(started_cluster):
    base_url_local_raw = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}"
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": base_url_local_raw,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )

def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
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

def test_metadata_cache(started_cluster_iceberg_no_spark):
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
    sort_order = SortOrder(SortField(source_id=4, transform=IdentityTransform()))
    table = catalog.create_table(
        identifier=f"{root_namespace}.test_metadata_cache",
        schema=schema,
        location=f"s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )

    data = []
    for _ in range(100):
        data.append(
            {
                "boolean_col": True,
                "long_col": 42,
                "double_col": 3.14,
                "string_col": "hello",
                "date_col": date.today(),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)

    query_id = f"iceberg-cache-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test_metadata_cache`",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    # First query: cache miss (metadata fetched from remote storage)
    cache_misses = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
    )
    assert cache_misses > 0, "First query should have cache misses"

    # Second query: cache hit (metadata served from cache)
    query_id = f"iceberg-cache-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test_metadata_cache`",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    cache_hits = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheHits'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
    )
    cache_misses = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
    )
    assert cache_hits > 0, "Second query should have cache hits"
    assert cache_misses == 0, "Second query should have no cache misses"

    # Clear cache and verify next query misses again
    instance.query("SYSTEM CLEAR ICEBERG METADATA CACHE")

    query_id = f"iceberg-cache-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test_metadata_cache`",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    cache_misses = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
    )
    assert cache_misses > 0, "Query after cache clear should have cache misses"

    # Query with cache disabled: neither hits nor misses
    query_id = f"iceberg-cache-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test_metadata_cache` "
        f"SETTINGS use_iceberg_metadata_files_cache='0'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    result = instance.query(
        f"SELECT ProfileEvents['IcebergMetadataFilesCacheHits'], "
        f"ProfileEvents['IcebergMetadataFilesCacheMisses'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ).strip()
    assert result == "0\t0", f"Cache disabled query should have no hits or misses, got: {result}"
