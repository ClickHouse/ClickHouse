from datetime import datetime, timezone
import time
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    generate_data,
    get_last_snapshot,
    get_uuid_str,
    get_uuid_str,
    write_iceberg_from_df,
    default_upload_directory
)



@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_iceberg_snapshot_reads(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_iceberg_snapshot_reads"
        + format_version
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
        format_version=format_version,
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    snapshot1_timestamp = datetime.now(timezone.utc)
    snapshot1_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    time.sleep(0.1)

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
        format_version=format_version,
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )
    snapshot2_timestamp = datetime.now(timezone.utc)
    snapshot2_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    time.sleep(0.1)

    write_iceberg_from_df(
        spark,
        generate_data(spark, 200, 300),
        TABLE_NAME,
        mode="append",
        format_version=format_version,
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )
    snapshot3_timestamp = datetime.now(timezone.utc)
    snapshot3_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(300)"
    )

    # Validate that each snapshot timestamp only sees the data inserted by that time.
    assert (
        instance.query(
            f"""
                          SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_timestamp_ms = {int(snapshot1_timestamp.timestamp() * 1000)}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(100)")
    )

    assert (
        instance.query(
            f"""
                          SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_snapshot_id = {snapshot1_id}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(100)")
    )


    assert (
        instance.query(
            f"""
                          SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_timestamp_ms = {int(snapshot2_timestamp.timestamp() * 1000)}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(200)")
    )

    assert (
        instance.query(
            f"""
                          SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_snapshot_id = {snapshot2_id}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(200)")
    )


    assert (
        instance.query(
            f"""SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_timestamp_ms = {int(snapshot3_timestamp.timestamp() * 1000)}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(300)")
    )

    assert (
        instance.query(
            f"""
                          SELECT * FROM {TABLE_NAME} ORDER BY 1
                          SETTINGS iceberg_snapshot_id = {snapshot3_id}"""
        )
        == instance.query("SELECT number, toString(number + 1) FROM numbers(300)")
    )