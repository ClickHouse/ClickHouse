#!/usr/bin/env python3

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3",])
def test_writes_multiple_threads(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    r1 = instance.is_built_with_llvm_coverage()
    if r1:
        pytest.skip("Flaky under llvm_coverage")
    TABLE_NAME = "test_writes_multiple_threads_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(x Int32)", format_version)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    query_id = get_uuid_str()
    instance.query(f"INSERT INTO {TABLE_NAME} SELECT number from system.numbers_mt LIMIT 4000000", settings={"allow_insert_into_iceberg": 1, "iceberg_insert_max_rows_in_data_file" : 4000000, "query_id": query_id, "max_insert_threads": 32})
    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == '4000000\n'
    assert instance.query(f"SELECT sum(x) FROM {TABLE_NAME}") == str(sum(range(4000000))) + '\n'
