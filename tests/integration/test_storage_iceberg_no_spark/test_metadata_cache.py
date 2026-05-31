#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from datetime import date
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

def get_profile_event(instance, query_id, event):
    return int(
        instance.query(
            f"SELECT ProfileEvents['{event}'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
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
    table_name = f"{root_namespace}.test_metadata_cache"
    table = catalog.create_table(
        identifier=table_name,
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

    # Phase 1: Cold query.  The REST catalog supplies catalog_uuid_hint, so the metadata cache
    # is probed with the known UUID before any remote read.  The entry is absent → miss.
    query_id = f"iceberg-cache-cold-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}`",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")

    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    cache_skipped = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheSkipped")
    assert cache_misses > 0, "First query should have cache misses (cold start)"
    assert cache_skipped == 0, (
        "First query must probe the cache (UUID from REST catalog_uuid_hint); "
        "non-zero skips mean UUID was not propagated"
    )

    # Phase 2: Second query on the same IcebergMetadata object — persistent_components already
    # holds table_uuid from phase 1, so the cache probe uses the known UUID → hit.
    query_id = f"iceberg-cache-warm-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}`",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")

    cache_hits = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheHits")
    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    cache_skipped = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheSkipped")
    assert cache_hits > 0, "Second query should have cache hits"
    assert cache_misses == 0, "Second query should have no cache misses"
    assert cache_skipped == 0, "Second query should not skip the cache probe"

    # Phase 3: Drop and recreate the database to force a fresh IcebergMetadata initialisation.
    # The REST catalog again supplies catalog_uuid_hint.  Because the cache is still warm the
    # probe should hit immediately — no remote read.  If catalog_uuid_hint propagation were
    # removed, getMetadataJSONObject would bypass the probe entirely
    # (IcebergMetadataFilesCacheSkipped > 0) and perform an unconditional remote read, causing
    # the assertion below to fail.
    instance.query(f"DROP DATABASE {CATALOG_NAME}")
    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)

    query_id = f"iceberg-cache-fresh-init-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}`",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")

    cache_hits = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheHits")
    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    cache_skipped = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheSkipped")
    assert cache_hits > 0, (
        "Fresh IcebergMetadata init should hit the cache when catalog_uuid_hint is propagated"
    )
    assert cache_misses == 0, (
        "Fresh IcebergMetadata init should not miss (cache was warm from phase 1)"
    )
    assert cache_skipped == 0, (
        "Fresh IcebergMetadata init must probe the cache via catalog_uuid_hint; "
        "non-zero skips prove the UUID was not propagated from the REST catalog"
    )

    # Phase 4: Clear cache and verify the next query misses again.
    instance.query("SYSTEM CLEAR ICEBERG METADATA CACHE")

    query_id = f"iceberg-cache-after-clear-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}`",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")

    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    assert cache_misses > 0, "Query after cache clear should have cache misses"

    # Phase 5: Cache disabled — neither hits, misses, nor skips.
    query_id = f"iceberg-cache-disabled-{uuid.uuid4()}"
    instance.query(
        f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}` "
        f"SETTINGS use_iceberg_metadata_files_cache='0'",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")

    result = instance.query(
        f"SELECT ProfileEvents['IcebergMetadataFilesCacheHits'], "
        f"ProfileEvents['IcebergMetadataFilesCacheMisses'], "
        f"ProfileEvents['IcebergMetadataFilesCacheSkipped'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    ).strip()
    assert result == "0\t0\t0", (
        f"Cache disabled query should have no hits, misses, or skips, got: {result}"
    )
