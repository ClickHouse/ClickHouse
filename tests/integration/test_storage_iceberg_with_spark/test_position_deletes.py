import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table,
    get_creation_expression
)


def get_array(query_result: str):
    arr = sorted([int(x) for x in query_result.strip().split("\n")])
    print(arr)
    return arr

@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_position_deletes(started_cluster_iceberg_with_spark, use_roaring_bitmaps,  storage_type, run_on_cluster):
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg PARTITIONED BY (bucket(5, id)) TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    expression = get_creation_expression(storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, run_on_cluster=run_on_cluster, table_function=True)

    print(expression)

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}
    assert int(instance.query(f"SELECT count() FROM {expression}", settings=settings)) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 100))

    # Check that filters are applied after deletes
    assert int(instance.query(f"SELECT count() FROM {expression} where id >= 15", settings=settings)) == 80
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression} where id >= 15 SETTINGS optimize_trivial_count_query=1",
                settings=settings,
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
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90))

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
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90)) + list(
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
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90)) + list(
        range(100, 150)
    )

    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id = 70 SETTINGS use_iceberg_partition_pruning = 1",
            settings=settings,
        )
    ) == [70]

    # Check PREWHERE (or regular WHERE if optimize_move_to_prewhere = 0 or
    # input_format_parquet_use_native_reader_v3 = 0)
    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id % 3 = 0", settings=settings)) == list(range(21, 90, 3)) + list(range(102, 150, 3))

@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_position_deletes_out_of_order(started_cluster_iceberg_with_spark, use_roaring_bitmaps):
    storage_type = "local"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_out_of_order_" + get_uuid_str()

    settings = {
        "use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps,
        "input_format_parquet_use_native_reader_v3": 1,
    }

    # There are a few flaky hacks chained together here.
    # We want the parquet reader to produce chunks corresponding to row groups out of order if
    # `format_settings.parquet.preserve_order` wasn't enabled. For that we:
    #  * Use `PREWHERE NOT sleepEachRow(...)` to make the reader take longer for bigger row groups.
    #  * Set spark row group size limit to 1 byte. Rely on current spark implementation detail:
    #    it'll check this limit every 100 rows. So effectively we've set row group size to 100 rows.
    #  * Insert 105 rows. So the first row group will have 100 rows, the second 5 rows.
    # If one of these steps breaks in future, this test will be less effective but won't fail.

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.parquet.row-group-size-bytes'='1')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select /*+ COALESCE(1) */ id, char(id + ascii('a')) from range(0, 105)")
    # (Fun fact: if you replace these two queries with one query with `WHERE id < 10 OR id = 103`,
    #  spark either quetly fails to delete row 103 or outright crashes with segfault in jre.)
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 10")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 103")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, additional_settings=list(map(lambda kv: f'{kv[0]}={kv[1]}', settings.items())))

    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} PREWHERE NOT sleepEachRow(1/100) order by id", settings=settings)) == list(range(10, 103)) + [104]

    instance.query(f"DROP TABLE {TABLE_NAME}")
