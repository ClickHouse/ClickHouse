import glob

import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_last_snapshot,
    get_uuid_str,
)

WAREHOUSE_DIR = "/var/lib/clickhouse/user_files/iceberg_data/default"


def publish(started_cluster, storage_type, table_name):
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )


def deletion_vector_files(table_name):
    return sorted(
        glob.glob(f"{WAREHOUSE_DIR}/{table_name}/**/*.puffin", recursive=True)
    )


def position_delete_files(table_name):
    return sorted(
        glob.glob(f"{WAREHOUSE_DIR}/{table_name}/**/*deletes*.parquet", recursive=True)
    )


def create_v3_merge_on_read_table(spark, table_name, schema="id bigint, data string", partition_by=""):
    partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    spark.sql(
        f"""
        CREATE TABLE {table_name} ({schema}) USING iceberg {partition_clause}
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )


def clickhouse_ids(instance, table_expression, where="", settings=None):
    raw = instance.query(
        f"SELECT id FROM {table_expression} {where}", settings=settings
    )
    return sorted(int(x) for x in raw.strip().split("\n") if x)


def spark_ids(spark, table_name):
    return sorted(row["id"] for row in spark.sql(f"SELECT id FROM {table_name}").collect())


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_deletion_vector_reads(
    started_cluster_iceberg_with_spark, storage_type, run_on_cluster, use_roaring_bitmaps
):
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deletion_vector_reads_" + storage_type + "_" + get_uuid_str()

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}

    create_v3_merge_on_read_table(spark, TABLE_NAME)
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)"
    )
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=run_on_cluster,
    )

    assert int(instance.query(f"SELECT count() FROM {expression}", settings=settings)) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert deletion_vector_files(TABLE_NAME), (
        "Spark did not write a deletion vector, the test does not check the deletion "
        "vector read path anymore"
    )
    assert not position_delete_files(TABLE_NAME)

    assert clickhouse_ids(instance, expression, settings=settings) == list(range(20, 100))
    assert clickhouse_ids(instance, expression, settings=settings) == spark_ids(spark, TABLE_NAME)

    assert int(
        instance.query(f"SELECT count() FROM {expression} WHERE id >= 15", settings=settings)
    ) == 80
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} WHERE id >= 15 SETTINGS optimize_trivial_count_query = 1",
            settings=settings,
        )
    ) == 80
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} SETTINGS optimize_trivial_count_query = 1",
            settings=settings,
        )
    ) == 80

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 90")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert clickhouse_ids(instance, expression, settings=settings) == list(range(20, 90))

    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)"
    )
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert clickhouse_ids(instance, expression, settings=settings) == list(
        range(20, 90)
    ) + list(range(100, 200))

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 150")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert clickhouse_ids(instance, expression, settings=settings) == list(
        range(20, 90)
    ) + list(range(100, 150))
    assert clickhouse_ids(instance, expression, settings=settings) == spark_ids(spark, TABLE_NAME)

    assert clickhouse_ids(
        instance, expression, where="WHERE id % 3 = 0", settings=settings
    ) == list(range(21, 90, 3)) + list(range(102, 150, 3))

    assert instance.query(
        f"SELECT data FROM {expression} WHERE id = 25", settings=settings
    ).strip() == chr(25 + ord("a"))


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_deletion_vector_on_partitioned_table(
    started_cluster_iceberg_with_spark, use_roaring_bitmaps
):
    storage_type = "s3"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deletion_vector_partitioned_" + get_uuid_str()

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}

    create_v3_merge_on_read_table(spark, TABLE_NAME, partition_by="bucket(5, id)")
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 100)"
    )
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id % 7 = 0")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert deletion_vector_files(TABLE_NAME)
    assert not position_delete_files(TABLE_NAME)

    expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    expected = [x for x in range(0, 100) if x % 7 != 0]
    assert clickhouse_ids(instance, expression, settings=settings) == expected
    assert clickhouse_ids(instance, expression, settings=settings) == spark_ids(spark, TABLE_NAME)

    assert clickhouse_ids(
        instance,
        expression,
        where="WHERE id = 42 SETTINGS use_iceberg_partition_pruning = 1",
        settings=settings,
    ) == []
    assert clickhouse_ids(
        instance,
        expression,
        where="WHERE id = 43 SETTINGS use_iceberg_partition_pruning = 1",
        settings=settings,
    ) == [43]


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_deletion_vector_time_travel(started_cluster_iceberg_with_spark, use_roaring_bitmaps):
    storage_type = "s3"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deletion_vector_time_travel_" + get_uuid_str()

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}

    create_v3_merge_on_read_table(spark, TABLE_NAME)
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 50)"
    )
    snapshot_before_delete = get_last_snapshot(f"{WAREHOUSE_DIR}/{TABLE_NAME}/")

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 10")
    snapshot_after_delete = get_last_snapshot(f"{WAREHOUSE_DIR}/{TABLE_NAME}/")
    assert snapshot_before_delete != snapshot_after_delete

    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)
    assert deletion_vector_files(TABLE_NAME)

    expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    assert clickhouse_ids(
        instance,
        expression,
        where=f"SETTINGS iceberg_snapshot_id = {snapshot_before_delete}",
        settings=settings,
    ) == list(range(0, 50))
    assert clickhouse_ids(
        instance,
        expression,
        where=f"SETTINGS iceberg_snapshot_id = {snapshot_after_delete}",
        settings=settings,
    ) == list(range(10, 50))
    assert clickhouse_ids(instance, expression, settings=settings) == list(range(10, 50))


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_deletion_vector_after_update_and_merge(
    started_cluster_iceberg_with_spark, use_roaring_bitmaps
):
    storage_type = "s3"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deletion_vector_update_merge_" + get_uuid_str()
    SOURCE_NAME = TABLE_NAME + "_source"

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}

    create_v3_merge_on_read_table(spark, TABLE_NAME)
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 40)"
    )
    spark.sql(f"UPDATE {TABLE_NAME} SET data = 'updated' WHERE id < 10")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert deletion_vector_files(TABLE_NAME)

    expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    assert clickhouse_ids(instance, expression, settings=settings) == list(range(0, 40))
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} WHERE data = 'updated'", settings=settings
        )
    ) == 10

    spark.sql(f"CREATE TABLE {SOURCE_NAME} (id bigint, data string) USING iceberg")
    spark.sql(
        f"INSERT INTO {SOURCE_NAME} select id, 'merged' from range(35, 45)"
    )
    spark.sql(
        f"""
        MERGE INTO {TABLE_NAME} t USING {SOURCE_NAME} s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.data = s.data
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert clickhouse_ids(instance, expression, settings=settings) == list(range(0, 45))
    assert clickhouse_ids(instance, expression, settings=settings) == spark_ids(spark, TABLE_NAME)
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} WHERE data = 'merged'", settings=settings
        )
    ) == 10

    spark.sql(f"DROP TABLE {SOURCE_NAME}")


@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_deletion_vector_large_cardinality(
    started_cluster_iceberg_with_spark, use_roaring_bitmaps
):
    storage_type = "s3"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_deletion_vector_large_cardinality_" + get_uuid_str()

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}

    create_v3_merge_on_read_table(spark, TABLE_NAME)
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select /*+ COALESCE(1) */ id, char(id % 26 + ascii('a')) from range(0, 100000)"
    )
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id % 2 = 0")
    publish(started_cluster_iceberg_with_spark, storage_type, TABLE_NAME)

    assert deletion_vector_files(TABLE_NAME)

    expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    assert int(instance.query(f"SELECT count() FROM {expression}", settings=settings)) == 50000
    assert int(
        instance.query(f"SELECT sum(id) FROM {expression}", settings=settings)
    ) == sum(range(1, 100000, 2))
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} WHERE id % 2 = 0", settings=settings
        )
    ) == 0
    assert int(
        instance.query(
            f"SELECT count() FROM {expression} WHERE id BETWEEN 1000 AND 1999",
            settings=settings,
        )
    ) == 500
