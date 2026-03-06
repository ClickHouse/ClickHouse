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
        "test_iceberg_getting_stale_data"
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
                         iceberg_metadata_async_refresh_period_ms=3_600_000)
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
                              "iceberg_metadata_staleness_seconds":600,
                              "log_comment":f"{TABLE_NAME}_01"
                              })) == 100

    # no reads from S3 should be performed as a part of this operation
    instance.query("SYSTEM FLUSH LOGS query_log")
    assert 0 == int(instance.query(f"""SELECT ProfileEvents['S3ReadRequestsCount']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_01'
    """).strip())


    # by default, SELECT will query remote catalog for the latest metadata
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "log_comment":f"{TABLE_NAME}_02"
                              })) == 200

    # some reads should occur as a part of this operation
    instance.query("SYSTEM FLUSH LOGS query_log")
    assert 0 < int(instance.query(f"""SELECT ProfileEvents['S3ReadRequestsCount']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_02'
    """).strip())


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

    # first we sleep to make the cached metadata to be measurably stale
    time.sleep(5)

    # we accept stale metadata at SELECT, and running it with using cached metadata only - no call to remote catalog
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_seconds":600,
                              "log_comment":f"{TABLE_NAME}_03"
                              })) == 200

    instance.query("SYSTEM FLUSH LOGS query_log")
    assert 0 == int(instance.query(f"""SELECT ProfileEvents['S3ReadRequestsCount']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_03'
    """).strip())

    # now we'll query it with the staleness tolerance less that we slept, the latest metadata should get pulled from remote catalog
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_seconds":4,
                              "log_comment":f"{TABLE_NAME}_04"
                              })) == 300

    instance.query("SYSTEM FLUSH LOGS query_log")
    s3_reads, iceberg_cache_stale_misses = instance.query(f"""SELECT ProfileEvents['S3ReadRequestsCount'], ProfileEvents['IcebergMetadataFilesCacheStaleMisses']
            FROM system.query_log
            WHERE type = 'QueryFinish' AND log_comment = '{TABLE_NAME}_04'
    """).strip().split('\t')
    assert int(s3_reads) > 0
    assert int(iceberg_cache_stale_misses) == 1


@pytest.mark.parametrize("storage_type", ["s3"])
def test_default_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_getting_stale_data"
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

    # The expection is that async metadata refresher starts for each table if the cache is enabled; default interval is DEFAULT_ICEBERG_METADATA_ASYNC_REFRESH_PERIOD=10sec
    # It could be explicitly set at table creation as iceberg_metadata_async_refresh_period_ms, but we're checking the default in this scenario
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
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100
    # sleeping twice the update interval to let the refresh finish even if the server is overloaded
    time.sleep(10 * 2)
    # we don't pull fresh metadata at SELECT, but the data is up to date because of the async refresh
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_async_metadata_refresh(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_async_refresh"
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

    ASYNC_METADATA_REFRESH_PERIOD_MS=5000
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, iceberg_metadata_async_refresh_period_ms=ASYNC_METADATA_REFRESH_PERIOD_MS)
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
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 100
    # Wait for the background async refresher to pick up the new metadata (2 periods of ASYNC_METADATA_REFRESH_PERIOD_MS)
    time.sleep(ASYNC_METADATA_REFRESH_PERIOD_MS/1000 * 2)

    # we expect some background activity against S3
    s3_reads_after = int(instance.query(
        "SELECT value FROM system.events WHERE name = 'S3ReadRequestsCount' SETTINGS system_events_show_zero_values=1"
    ))
    assert s3_reads_after > s3_reads_before
    # we don't pull fresh metadata at SELECT, but the data is up to date because of the async refresh
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_staleness_seconds=600")) == 200


@pytest.mark.parametrize("storage_type", ["s3"])
def test_insert_updates_metadata_cache(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = (
        "test_iceberg_write_updates_metadata"
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    schema = "(a Int64)"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, iceberg_metadata_async_refresh_period_ms=3_600_000)

    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number FROM numbers(100)",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", settings={
                              "iceberg_metadata_staleness_seconds":600,
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
