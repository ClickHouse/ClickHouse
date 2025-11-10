import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table
)


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_position_deletes(started_cluster_iceberg_with_spark, use_roaring_bitmaps,  storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()
    instance.query(f"SET use_roaring_bitmap_iceberg_positional_deletes={use_roaring_bitmaps};")

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg PARTITIONED BY (bucket(5, id)) TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    def get_array(query_result: str):
        arr = sorted([int(x) for x in query_result.strip().split("\n")])
        print(arr)
        return arr

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}")) == list(range(20, 100))

    # Check that filters are applied after deletes
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} where id >= 15")) == 80
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} where id >= 15 SETTINGS optimize_trivial_count_query=1"
            )
        )
        == 80
    )

    # Check deletes after deletes
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 90")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}")) == list(range(20, 90))

    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD truncate(1, data)")

    # Check adds after deletes
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)"
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}")) == list(range(20, 90)) + list(
        range(100, 200)
    )

    # Check deletes after adds
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 150")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME}")) == list(range(20, 90)) + list(
        range(100, 150)
    )

    assert get_array(
        instance.query(
            f"SELECT id FROM {TABLE_NAME} WHERE id = 70 SETTINGS use_iceberg_partition_pruning = 1"
        )
    ) == [70]

    # Clean up
    instance.query(f"DROP TABLE {TABLE_NAME}")