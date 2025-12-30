import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    get_uuid_str,
    create_iceberg_table
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_delete_files(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_delete_files_"
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
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # Test trivial count with deleted files
    query_id = "test_trivial_count_" + get_uuid_str()
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", query_id=query_id)) == 100
    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(f"SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied'] FROM system.query_log where query_id = '{query_id}' and type = 'QueryFinish'") == "1\n"

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 0")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    query_id = "test_trivial_count_" + get_uuid_str()
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", query_id=query_id)) == 0

    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(f"SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied'] FROM system.query_log where query_id = '{query_id}' and type = 'QueryFinish'") == "1\n"

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="upsert",
        format_version=format_version,
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    query_id = "test_trivial_count_" + get_uuid_str()
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", query_id=query_id)) == 100

    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(f"SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied'] FROM system.query_log where query_id = '{query_id}' and type = 'QueryFinish'") == "1\n"

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 150")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    query_id = "test_trivial_count_" + get_uuid_str()
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}", query_id=query_id)) == 50

    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(f"SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied'] FROM system.query_log where query_id = '{query_id}' and type = 'QueryFinish'") == "1\n"
