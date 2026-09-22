import io

import pytest

from minio.commonconfig import CopySource

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)

# One row per data file, so a read has to open this many separate objects.
NUM_DATA_FILES = 40


def run_and_get_profile_events(instance, query, query_id, settings, events):
    """Run `query` and return its result together with its own values for `events`."""
    result = instance.query(query, query_id=query_id, settings=settings)
    instance.query("SYSTEM FLUSH LOGS")
    selected = ", ".join(f"ProfileEvents['{event}']" for event in events)
    row = instance.query(
        f"""
        SELECT {selected}
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    ).strip()
    return result, [int(value) for value in row.split("\t")]


def create_table_with_one_row_per_data_file(instance, cluster, table_name, num_files):
    """An Iceberg table of `num_files` data files, so a read has to open that many objects."""
    create_iceberg_table("s3", instance, table_name, cluster, "(x Int32)")
    instance.query(
        f"INSERT INTO {table_name} SELECT number FROM numbers({num_files})",
        settings={
            "iceberg_insert_max_rows_in_data_file": 1,
            "max_block_size": 1,
            "min_insert_block_size_rows": 1,
            "min_insert_block_size_bytes": 1,
        },
    )
    assert int(instance.query(f"SELECT uniqExact(_path) FROM {table_name}")) == num_files


@pytest.mark.parametrize("storage_type", ["s3"])
def test_manifest_object_metadata_avoids_head_request_per_data_file(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_manifest_object_metadata_" + storage_type + "_" + get_uuid_str()
    create_table_with_one_row_per_data_file(
        instance, started_cluster_iceberg_no_spark, table_name, NUM_DATA_FILES
    )

    # Need a query that iceberg can't trivially answer from metadata
    read_query = f"SELECT sum(x) FROM {table_name}"

    # Cache warming execution
    instance.query(read_query, settings={"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 0})

    profiled_events = ("S3HeadObject", "IcebergManifestObjectMetadataUsed")
    enabled_result, (enabled_heads, enabled_used) = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_enabled",
        settings={"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 0},
        events=profiled_events,
    )
    disabled_result, (disabled_heads, disabled_used) = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_disabled",
        settings={"use_iceberg_manifest_object_metadata": 0},
        events=profiled_events,
    )

    # This option should not change correctness.
    assert enabled_result == disabled_result

    assert enabled_used == NUM_DATA_FILES
    assert disabled_used == 0

    assert disabled_heads - enabled_heads == NUM_DATA_FILES


def test_manifest_object_metadata_yields_to_etag_validation(
    started_cluster_iceberg_no_spark,
):
    """With `s3_validate_etag_on_read` on the shortcut must not fire: that setting needs an ETag the
    manifest cannot supply, so the read costs exactly what it costs with the shortcut disabled."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_manifest_object_metadata_validated_" + get_uuid_str()
    create_table_with_one_row_per_data_file(
        instance, started_cluster_iceberg_no_spark, table_name, NUM_DATA_FILES
    )

    read_query = f"SELECT sum(x) FROM {table_name}"
    profiled_events = ("S3HeadObject", "IcebergManifestObjectMetadataUsed")

    # Cache warming execution
    instance.query(read_query)

    validated_result, (validated_heads, validated_used) = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_validated",
        settings={"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 1},
        events=profiled_events,
    )
    disabled_result, (disabled_heads, disabled_used) = run_and_get_profile_events(
        instance,
        read_query,
        query_id=f"{table_name}_disabled",
        settings={"use_iceberg_manifest_object_metadata": 0, "s3_validate_etag_on_read": 1},
        events=profiled_events,
    )

    assert validated_result == disabled_result
    assert validated_used == 0
    assert disabled_used == 0
    assert validated_heads == disabled_heads


def test_manifest_object_metadata_still_fetches_etag_and_time_when_requested(
    started_cluster_iceberg_no_spark,
):
    """`_etag` and `_time` must come from the object store whether selected or only filtered on.
    Were the guard lost, `_etag` would quietly come back empty with the shortcut on."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_manifest_object_metadata_virtuals_" + get_uuid_str()
    num_files = 4
    create_table_with_one_row_per_data_file(
        instance, started_cluster_iceberg_no_spark, table_name, num_files
    )

    shortcut_on = {"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 0}
    shortcut_off = {"use_iceberg_manifest_object_metadata": 0}
    used = ("IcebergManifestObjectMetadataUsed",)

    # Control: without a virtual column in the way, every file is answered from the manifest.
    _, (control_used,) = run_and_get_profile_events(
        instance,
        f"SELECT sum(x) FROM {table_name}",
        query_id=f"{table_name}_control",
        settings=shortcut_on,
        events=used,
    )
    assert control_used == num_files

    # Selected: identical to a read that never took the shortcut.
    selected_query = f"SELECT _path, _etag, _time FROM {table_name} ORDER BY _path"
    with_shortcut, (selected_used,) = run_and_get_profile_events(
        instance, selected_query, f"{table_name}_selected", shortcut_on, used
    )
    assert selected_used == 0
    assert with_shortcut == instance.query(selected_query, settings=shortcut_off)
    for row in with_shortcut.strip().split("\n"):
        _, etag, time = row.split("\t")
        assert etag != "", row
        assert time != "1970-01-01 00:00:00", row

    # Filtered only: `requested_virtual_columns` has to carry a column that appears in no select list.
    for filtered_query in (
        f"SELECT count() FROM {table_name} WHERE _etag != ''",
        f"SELECT count() FROM {table_name} WHERE _time > toDateTime('2000-01-01 00:00:00')",
    ):
        matched, (filtered_used,) = run_and_get_profile_events(
            instance, filtered_query, f"{table_name}_{get_uuid_str()}", shortcut_on, used
        )
        assert filtered_used == 0, filtered_query
        assert int(matched) == num_files, filtered_query


# The content caches are server-wide and key on the data file's bucket-relative path plus a token,
# so two tables sharing a relative path are told apart by the storage namespace alone. Each case
# enables one cache, names the profile event proving the second bucket was read on its own terms,
# and uses a query that consults that cache. The page cache is only used without a filesystem cache.
CACHE_CASES = {
    "parquet_metadata": (
        {"use_parquet_metadata_cache": 1},
        "ParquetMetadataCacheMisses",
        "SELECT x FROM {table}",
    ),
    "filesystem": (
        {"enable_filesystem_cache": 1, "filesystem_cache_name": "cache1"},
        "CachedReadBufferCacheWriteBytes",
        "SELECT x FROM {table}",
    ),
    "page": (
        {"use_page_cache_for_object_storage": 1},
        "PageCacheMisses",
        "SELECT x FROM {table}",
    ),
    # Consulted only with a filter. `x > 0` matches in both tables and neither one's statistics prune
    # it, so caching is the only thing that can differ.
    "query_condition": (
        {"use_query_condition_cache": 1},
        "QueryConditionCacheMisses",
        "SELECT x FROM {table} WHERE x > 0",
    ),
}


def _table_prefix(table_name):
    return f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"


def _read_object(cluster, bucket, key):
    response = cluster.minio_client.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _sole_data_file_key(instance, table_query, bucket):
    """The single data file behind `table_query`, as a key relative to its bucket."""
    paths = instance.query(f"SELECT _path FROM {table_query}").split()
    assert len(paths) == 1, paths
    # `_path` is prefixed with the bucket, which is not part of the object key.
    assert paths[0].startswith(f"{bucket}/"), paths[0]
    return paths[0][len(bucket) + 1 :]


def _build_tables_colliding_across_buckets(cluster, instance, suffix):
    """Two one-row Iceberg tables holding different rows at the same relative data file path: a
    local table in `minio_bucket` and a table function over `minio_bucket_2`."""
    table_name = "test_manifest_object_metadata_alias_" + suffix
    other_content_table = "test_manifest_object_metadata_other_" + suffix

    for name, value in ((table_name, 1), (other_content_table, 2)):
        create_iceberg_table("s3", instance, name, cluster, "(x Int32)")
        instance.query(f"INSERT INTO {name} VALUES ({value})")

    # Written by the same writer with the same schema and value width, so the two data files are the
    # same size - the manifest copied alongside keeps recording a truthful `file_size_in_bytes`.
    data_file_key = _sole_data_file_key(instance, table_name, cluster.minio_bucket)
    other_content = _read_object(
        cluster,
        cluster.minio_bucket,
        _sole_data_file_key(instance, other_content_table, cluster.minio_bucket),
    )
    assert len(other_content) == len(
        _read_object(cluster, cluster.minio_bucket, data_file_key)
    )

    # Copy the table into the second bucket under identical keys, then give the data file different
    # contents: the collision a writer with deterministic file names reaches on its own.
    for obj in cluster.minio_client.list_objects(
        cluster.minio_bucket, _table_prefix(table_name), recursive=True
    ):
        cluster.minio_client.copy_object(
            cluster.minio_bucket_2,
            obj.object_name,
            CopySource(cluster.minio_bucket, obj.object_name),
        )
    cluster.minio_client.put_object(
        cluster.minio_bucket_2,
        data_file_key,
        io.BytesIO(other_content),
        len(other_content),
    )

    in_second_bucket = get_creation_expression(
        "s3", table_name, cluster, table_function=True, bucket=cluster.minio_bucket_2
    )
    return table_name, in_second_bucket


@pytest.mark.parametrize("cache_name", sorted(CACHE_CASES))
def test_manifest_object_metadata_does_not_alias_another_bucket(
    started_cluster_iceberg_no_spark, cache_name
):
    """Two Iceberg data files at the same relative path in two buckets are different objects.

    Each cache gets its own pair of tables, so an entry written for one case cannot answer another.
    """
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    cache_settings, miss_event, read_query = CACHE_CASES[cache_name]

    table_name, in_second_bucket = _build_tables_colliding_across_buckets(
        cluster, instance, get_uuid_str()
    )
    settings = {"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 0, **cache_settings}

    assert (
        instance.query(read_query.format(table=table_name), settings=settings).strip()
        == "1"
    )

    # The row catches served bytes; the miss catches a reused footer or skip marks, which can still
    # decode to the right row by coincidence when the two files share a layout.
    second_bucket_rows, (misses,) = run_and_get_profile_events(
        instance,
        read_query.format(table=in_second_bucket),
        query_id=f"{table_name}_second_bucket",
        settings=settings,
        events=(miss_event,),
    )
    assert second_bucket_rows.strip() == "2"
    assert misses > 0, f"{cache_name}: the second bucket was answered from the first bucket's entry"

    # And the reverse direction.
    assert (
        instance.query(read_query.format(table=table_name), settings=settings).strip()
        == "1"
    )


@pytest.mark.parametrize("cache_name", sorted(CACHE_CASES))
def test_manifest_object_metadata_keeps_the_content_caches_usable(
    started_cluster_iceberg_no_spark, cache_name
):
    """The manifest-derived metadata must still let the caches identify the contents. Reporting them
    as unidentifiable would silently disable every cache below for all Iceberg reads."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    cache_settings, _, read_query = CACHE_CASES[cache_name]
    hit_event = {
        "parquet_metadata": "ParquetMetadataCacheHits",
        "filesystem": "CachedReadBufferReadFromCacheBytes",
        "page": "PageCacheHits",
        "query_condition": "QueryConditionCacheHits",
    }[cache_name]

    table_name = "test_manifest_object_metadata_cacheable_" + get_uuid_str()
    create_iceberg_table("s3", instance, table_name, cluster, "(x Int32)")
    instance.query(f"INSERT INTO {table_name} SELECT number + 1 FROM numbers(16)")

    settings = {"use_iceberg_manifest_object_metadata": 1, "s3_validate_etag_on_read": 0, **cache_settings}
    query = read_query.format(table=table_name)

    instance.query(query, settings=settings)
    _, (hits,) = run_and_get_profile_events(
        instance,
        query,
        query_id=f"{table_name}_{cache_name}",
        settings=settings,
        events=(hit_event,),
    )
    assert hits > 0, f"{cache_name}: {hit_event} did not move, the cache was skipped"
