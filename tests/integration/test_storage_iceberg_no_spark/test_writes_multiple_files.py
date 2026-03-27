import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_multiple_files(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_multiple_files_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(x Int32)", format_version)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    data = ""
    expected_result = ""
    for i in range(1058449):
        data += f"({i}), "
        expected_result += f"{i}\n"
    data = data[:len(data) - 2]
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES {data};", settings={"allow_insert_into_iceberg": 1, "iceberg_insert_max_rows_in_data_file" : 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == expected_result

    files = default_download_directory(
        started_cluster_iceberg_no_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    assert sum([file[-7:] == "parquet" for file in files]) == 2
