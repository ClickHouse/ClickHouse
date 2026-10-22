#!/usr/bin/env python3

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3",])
def test_writes_multiple_threads(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_multiple_threads_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Int32, v String)", format_version, use_version_hint=True)

    instance.query(f"INSERT INTO {TABLE_NAME} SELECT number, toString(number) from system.numbers_mt LIMIT 400", settings={"allow_insert_into_iceberg": 1, "iceberg_insert_max_rows_in_data_file" : 10, "max_insert_threads": 32, "max_insert_block_size": 10, "max_block_size": 10})
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == '400\n'
    assert instance.query(f"SELECT sum(x) FROM {TABLE_NAME}") == str(sum(range(400))) + '\n'

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    spark = started_cluster_iceberg_with_spark.spark_session
    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 400
