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
        location=f"s3://warehouse-rest/data/{root_namespace}/test_metadata_cache",
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

def test_metadata_cache_fresh_init_schemeless_location(started_cluster_iceberg_no_spark):
    """
    The main test_metadata_cache above only exercises the fresh-init cache hit against a
    full-URI `location` (`s3://...`), because the table there is created by PyIceberg with an
    explicit `location=`. That is not the format ClickHouse itself writes by default
    (`write_full_path_in_iceberg_metadata=0` writes a schemeless `location`), and the warm
    fresh-init hit for that default format depends on more than `cachedLocationMatchesTableRoot`
    alone: `getDataSourceDescription`, namespace derivation, and the cache-probe acceptance path
    all have to agree. Exercise the same cold/warm/fresh-init sequence against a table whose
    metadata.json is entirely ClickHouse-written with the default (schemeless) location format.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    table_name = f"{root_namespace}.test_metadata_cache_schemeless"
    table_ref = f"{CATALOG_NAME}.`{table_name}`"

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)

    # The REST catalog server itself writes the very first metadata.json on CREATE TABLE (it
    # commits table creation server-side via its own S3 file IO), so that initial location must
    # be a full URI regardless of write_full_path_in_iceberg_metadata -- a schemeless location at
    # this step is rejected by the REST server with "Invalid S3 URI, cannot determine scheme".
    instance.query(
        f"""
CREATE TABLE {table_ref} (string_col String)
ENGINE = IcebergS3('http://minio1:9001/warehouse-rest/{table_name}/', '{minio_access_key}', '{minio_secret_key}')
""",
        settings={"allow_experimental_database_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )
    # INSERT, unlike CREATE TABLE, is entirely client-driven: ClickHouse writes the new
    # metadata.json itself and only tells the REST catalog the new pointer via updateMetadata, so
    # this is where the default (write_full_path_in_iceberg_metadata=0) schemeless `location`
    # actually gets exercised.
    instance.query(
        f"INSERT INTO {table_ref} VALUES ('hello')",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Phase 1: Cold query.
    query_id = f"iceberg-cache-schemeless-cold-{uuid.uuid4()}"
    instance.query(f"SELECT string_col FROM {table_ref}", query_id=query_id)
    instance.query("SYSTEM FLUSH LOGS")

    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    assert cache_misses > 0, "First query should have cache misses (cold start)"

    # Phase 2: Second query on the same IcebergMetadata object -> hit.
    query_id = f"iceberg-cache-schemeless-warm-{uuid.uuid4()}"
    instance.query(f"SELECT string_col FROM {table_ref}", query_id=query_id)
    instance.query("SYSTEM FLUSH LOGS")

    cache_hits = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheHits")
    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    assert cache_hits > 0, "Second query should have cache hits"
    assert cache_misses == 0, "Second query should have no cache misses"

    # Phase 3: Drop and recreate the database to force a fresh IcebergMetadata initialisation.
    # The cache is still warm, and the table's metadata.json has a schemeless `location`, so the
    # fresh-init probe must still hit: cachedLocationMatchesTableRoot no longer needs to verify
    # table_namespace for schemeless locations, and the cache key is namespaced by
    # getDataSourceDescription, so a hit can only come from this exact backend.
    instance.query(f"DROP DATABASE {CATALOG_NAME}")
    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)

    query_id = f"iceberg-cache-schemeless-fresh-init-{uuid.uuid4()}"
    instance.query(f"SELECT string_col FROM {table_ref}", query_id=query_id)
    instance.query("SYSTEM FLUSH LOGS")

    cache_hits = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheHits")
    cache_misses = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses")
    cache_skipped = get_profile_event(instance, query_id, "IcebergMetadataFilesCacheSkipped")
    assert cache_hits > 0, (
        "Fresh IcebergMetadata init should hit the cache for a schemeless-location table"
    )
    assert cache_misses == 0, (
        "Fresh IcebergMetadata init should not miss (cache was warm from phase 1)"
    )
    assert cache_skipped == 0, (
        "Fresh IcebergMetadata init must not fall back to an unconditional remote read: an "
        "unconditional read would record Skipped here and retroactively populate the cache, "
        "masking a regression where cache_hits > 0 only because of that after-the-fact insert"
    )


def test_system_iceberg_tables_are_not_affected_by_metadata_cache(
    started_cluster_iceberg_no_spark,
):
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/pull/89003: `system.iceberg_history`
    used to report one table's snapshots for a different table, because the metadata cache was keyed
    by the table path alone. `StorageSystemIcebergHistory` works around it by forcing
    `use_iceberg_metadata_files_cache = 0`, so this test pins down both that the setting is still
    honoured (no cache hits/misses are recorded while filling the system table) and that each table
    gets its own snapshots even when two databases over the same catalog have the cache warm.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(field_id=1, name="string_col", field_type=StringType(), required=False),
    )
    tables = {}
    for index, short_name in enumerate(["system_tables_a", "system_tables_b"], start=1):
        table_name = f"{root_namespace}.{short_name}"
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            location=f"s3://warehouse-rest/data/{root_namespace}/{short_name}",
        )
        # Different number of appends per table, so a cross-table mix-up shows up as a wrong
        # snapshot count rather than only as a wrong snapshot id.
        for _ in range(index):
            table.append(pa.Table.from_pylist([{"string_col": short_name}]))
        tables[short_name] = table_name

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)

    # Warm the metadata cache for both tables, so the system table would be served from it if it
    # ignored `use_iceberg_metadata_files_cache`.
    for table_name in tables.values():
        instance.query(f"SELECT string_col FROM {CATALOG_NAME}.`{table_name}`")

    query_id = f"iceberg-system-history-{uuid.uuid4()}"
    history = instance.query(
        f"SELECT table, count(), uniqExact(snapshot_id) FROM system.iceberg_history "
        f"WHERE database = '{CATALOG_NAME}' AND table LIKE '{root_namespace}%' "
        f"GROUP BY table ORDER BY table",
        query_id=query_id,
    )
    assert history == (
        f"{tables['system_tables_a']}\t1\t1\n" f"{tables['system_tables_b']}\t2\t2\n"
    ), f"Unexpected system.iceberg_history contents: {history}"

    instance.query("SYSTEM FLUSH LOGS")
    assert (
        get_profile_event(instance, query_id, "IcebergMetadataFilesCacheHits") == 0
        and get_profile_event(instance, query_id, "IcebergMetadataFilesCacheMisses") == 0
    ), "system.iceberg_history must not consult the metadata cache (use_iceberg_metadata_files_cache=0)"

    # `system.iceberg_files` walks the same metadata; each table must see only its own data files.
    files = instance.query(
        f"SELECT table, count() FROM system.iceberg_files "
        f"WHERE database = '{CATALOG_NAME}' AND table LIKE '{root_namespace}%' "
        f"GROUP BY table ORDER BY table"
    )
    assert files == (
        f"{tables['system_tables_a']}\t1\n" f"{tables['system_tables_b']}\t2\n"
    ), f"Unexpected system.iceberg_files contents: {files}"
