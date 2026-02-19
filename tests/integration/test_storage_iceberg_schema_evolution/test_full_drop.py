
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_full_drop(started_cluster_iceberg_schema_evolution, format_version, storage_type):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = "test_full_drop_" + storage_type + "_" + get_uuid_str()
    TABLE_NAME_2 = "test_full_drop_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_schema_evolution, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'

    create_iceberg_table(storage_type, instance, TABLE_NAME_2, started_cluster_iceberg_schema_evolution, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME_2} VALUES (777);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == '777\n'

    instance.query(f"DROP TABLE {TABLE_NAME}")

    #exception means that there are no files.
    with pytest.raises(Exception):
        default_download_directory(
            started_cluster_iceberg_schema_evolution,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == '777\n'
