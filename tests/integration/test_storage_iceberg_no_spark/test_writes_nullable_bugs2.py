import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_nullable_bugs2(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_nullable_bugs2_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(c0 Nullable(String))", format_version, "(c0)")
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (NULL), ('Monetochka'), ('Maneskin'), ('Noize MC');", settings={"allow_experimental_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'Maneskin\nMonetochka\nNoize MC\n\\N\n'
