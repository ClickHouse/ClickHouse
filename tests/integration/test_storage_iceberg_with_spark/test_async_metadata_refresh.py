import time
import pytest
from helpers.iceberg_utils import (
    create_iceberg_table,
    generate_data,
    get_uuid_str,
    write_iceberg_from_df,
    default_upload_directory,
)

_ASYNC_CACHE_REFRESH_CONFIG_PATH = "/etc/clickhouse-server/config.d/iceberg_async_cache_refresh.xml"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_selecting_with_stale_vs_latest_metadata(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_selecting_with_stale_vs_latest_metadata"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # disabling async refresher to validate that the latest metadata will be pulled at SELECT
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark,
        additional_settings = [
            f"iceberg_metadata_async_prefetch_period_ms=0"
    ])
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    # 1. Validating that SELECT will pull the latest metadata by default
    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # Now we will SELECT data with accepting a stale metadata - the expectation is that no call to remote catalog will occur and the cached metadata to be used
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_ms":600_000,
                              "log_comment":f"{TABLE_NAME}_01"
                              })) == 100

    # no reads from S3 should be performed as a part of this operation
    instance.query("SYSTEM FLUSH LOGS query_log")
    s3_read, s3_get, s3_head, s3_list, cache_hit, cache_miss, cache_stale_miss = instance.query(f"""
        SELECT
            ProfileEvents['S3ReadRequestsCount'],
            ProfileEvents['S3GetObject'],
            ProfileEvents['S3HeadObject'],
            ProfileEvents['S3ListObjects'],
            ProfileEvents['IcebergMetadataFilesCacheHits'],
            ProfileEvents['IcebergMetadataFilesCacheMisses'],
            ProfileEvents['IcebergMetadataFilesCacheStaleMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_01'
    """).strip().split('\t')
    # nothing has been requested from the remote catalog (s3)
    assert 0 == int(s3_read)
    assert 0 == int(s3_get)
    assert 0 == int(s3_head)
    assert 0 == int(s3_list)
    # cache hits only, no misses
    assert 0 < int(cache_hit)
    assert 0 == int(cache_miss)
    assert 0 == int(cache_stale_miss)


    # by default, SELECT will query remote catalog for the latest metadata
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "log_comment":f"{TABLE_NAME}_02"
                              })) == 200

    # some reads should occur as a part of this operation
    instance.query("SYSTEM FLUSH LOGS query_log")
    s3_read, s3_get, s3_head, s3_list, cache_hit, cache_miss, cache_stale_miss = instance.query(f"""
        SELECT
            ProfileEvents['S3ReadRequestsCount'],
            ProfileEvents['S3GetObject'],
            ProfileEvents['S3HeadObject'],
            ProfileEvents['S3ListObjects'],
            ProfileEvents['IcebergMetadataFilesCacheHits'],
            ProfileEvents['IcebergMetadataFilesCacheMisses'],
            ProfileEvents['IcebergMetadataFilesCacheStaleMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_02'
    """).strip().split('\t')
    assert 0 < int(s3_read)
    assert 0 < int(s3_get)
    assert 0 < int(s3_head)
    assert 0 < int(s3_list)
    assert 0 < int(cache_hit)   # old manifest lists & files are found in cache
    assert 0 < int(cache_miss)  # new manifest lists & files are not found in local cache
    assert 0 == int(cache_stale_miss)

    # 2. Validating that SELECT will pull the latest metadata if the cached version is stale
    write_iceberg_from_df(
        spark,
        generate_data(spark, 200, 300),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # first, we sleep to make the cached metadata to be measurably stale
    time.sleep(5)

    # then, we accept really stale metadata at SELECT - no call to remote catalog
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_ms":600_000,
                              "log_comment":f"{TABLE_NAME}_03"
                              })) == 200

    instance.query("SYSTEM FLUSH LOGS query_log")
    # nothing has been queried from s3
    assert 0 == int(instance.query(f"""SELECT ProfileEvents['S3ReadRequestsCount']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_03'
    """).strip())

    # lastly, we SELECT with tiny tolerance to stale metadata - latest metadata will be fetched from s3
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_ms":4_000,
                              "log_comment":f"{TABLE_NAME}_04"
                              })) == 300
    # latest metadata was fetched from s3
    instance.query("SYSTEM FLUSH LOGS query_log")
    s3_read, s3_get, s3_head, s3_list, cache_hit, cache_miss, cache_stale_miss = instance.query(f"""
        SELECT
            ProfileEvents['S3ReadRequestsCount'],
            ProfileEvents['S3GetObject'],
            ProfileEvents['S3HeadObject'],
            ProfileEvents['S3ListObjects'],
            ProfileEvents['IcebergMetadataFilesCacheHits'],
            ProfileEvents['IcebergMetadataFilesCacheMisses'],
            ProfileEvents['IcebergMetadataFilesCacheStaleMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_04'
    """).strip().split('\t')
    assert 0 < int(s3_read)
    assert 0 < int(s3_get)
    assert 0 < int(s3_head)
    assert 0 < int(s3_list)
    assert 0 < int(cache_hit)   # old manifest lists & files are found in cache
    assert 0 < int(cache_miss)  # new manifest lists & files are not found in local cache
    assert 0 < int(cache_stale_miss) # the cached metadata has been considered stale


@pytest.mark.parametrize("storage_type", ["s3"])
def test_default_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_default_async_metadata_refresh"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # The expectation is that async metadata fetcher is disabled by default
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    # the fresh metadata won't get pulled at SELECT, so we see stale data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_ms=600000")) == 100
    # sleeping a little bit
    time.sleep(10)
    # the metadata is not updated after sleep
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_ms=600000")) == 100


@pytest.mark.parametrize("storage_type", ["s3"])
def test_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_async_metadata_refresh"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    ASYNC_METADATA_REFRESH_PERIOD_MS=5_000
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark,
        additional_settings = [
            f"iceberg_metadata_async_prefetch_period_ms={ASYNC_METADATA_REFRESH_PERIOD_MS}"
    ])
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    # In order to track background activity against S3, let's remember current metric
    s3_reads_before = int(instance.query(
        "SELECT value FROM system.events WHERE name = 'S3ReadRequestsCount' SETTINGS system_events_show_zero_values=1"
    ))

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )
    # the fresh metadata won't get pulled at SELECT, so we see stale data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_ms=600000")) == 100
    # Wait for the background async refresher to pick up the new metadata (2 periods of ASYNC_METADATA_REFRESH_PERIOD_MS)
    time.sleep(ASYNC_METADATA_REFRESH_PERIOD_MS/1000 * 2)

    # we expect some background activity against S3
    s3_reads_after = int(instance.query(
        "SELECT value FROM system.events WHERE name = 'S3ReadRequestsCount' SETTINGS system_events_show_zero_values=1"
    ))
    assert s3_reads_after > s3_reads_before
    # we don't pull fresh metadata at SELECT, but the data is up to date because of the async refresh
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_ms=600000")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_insert_updates_metadata_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = (
        "test_insert_updates_metadata_cache"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    schema = "(a Int64)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema,
        additional_settings = [
            f"iceberg_metadata_async_prefetch_period_ms=0"
    ])

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_ms":600_000,
                              "log_comment":f"{TABLE_NAME}_40"
                              })) == 100

    instance.query("SYSTEM FLUSH LOGS query_log")
    s3_reads, iceberg_cache_stale_misses, iceberg_cache_misses = instance.query(f"""
            SELECT ProfileEvents['S3ReadRequestsCount'], ProfileEvents['IcebergMetadataFilesCacheStaleMisses'], ProfileEvents['IcebergMetadataFilesCacheMisses']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_40'
    """).strip().split('\t')
    assert int(s3_reads) > 0
    assert int(iceberg_cache_misses) > 0
    assert int(iceberg_cache_stale_misses) == 0
