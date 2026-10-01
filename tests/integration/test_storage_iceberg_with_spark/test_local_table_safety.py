import pytest

from helpers.iceberg_utils import (
    write_iceberg_from_df,
    generate_data,
    get_uuid_str
)

from helpers.s3_tools import (
    LocalUploader,
)

def test_local_table_safety(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_local_table_safety_" + get_uuid_str()
    )

    downloader = LocalUploader(instance, use_relpath=True)

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)

    downloader.upload_directory(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/"
    )

    table_function_expr = f"icebergLocal(local, path = '/iceberg_data/default/{TABLE_NAME}', format='Parquet')"

    error = instance.query_and_get_error(f"SELECT * FROM {table_function_expr}")

    assert "PATH_ACCESS_DENIED" in error
