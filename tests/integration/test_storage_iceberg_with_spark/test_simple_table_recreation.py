import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    create_iceberg_table,
    get_uuid_str,
    drop_iceberg_table
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_simple_table_recreation(started_cluster_iceberg_with_spark, storage_type):
    """
    Simple test that:
    1. Creates a table with Spark
    2. Inserts data and queries it
    3. Recreates the table with same approach
    4. Verifies the query still works with metadata cache
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_simple_table_recreation_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    # Step 1: Create initial table with Spark
    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, name string, value double) USING iceberg TBLPROPERTIES ('format-version' = '2')"
    )

    # Step 2: Insert sample data
    spark.sql(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.25), (3, 'Charlie', 300.75)"
    )

    # Upload data to storage
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # Step 3: Create ClickHouse table and query initial data
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # Query the initial data
    initial_count = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    initial_sum = float(instance.query(f"SELECT sum(value) FROM {TABLE_NAME}"))
    initial_names = instance.query(f"SELECT name FROM {TABLE_NAME} ORDER BY id").strip()

    # Verify initial state
    assert initial_count == 3
    assert initial_sum == 601.5
    assert initial_names == "Alice\nBob\nCharlie"

    # Step 5: Recreate the table using spark (drop and create again)
    drop_iceberg_table(instance, TABLE_NAME, if_exists=True)

    spark.sql(
        f"DROP TABLE IF EXISTS {TABLE_NAME}"
    )

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, name string, value double) USING iceberg TBLPROPERTIES ('format-version' = '2')"
    )

    spark.sql(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.25), (3, 'Charlie', 300.75)"
    )

    # Upload data to storage twice
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    # Query the second data
    final_count = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    final_sum = float(instance.query(f"SELECT sum(value) FROM {TABLE_NAME}"))
    final_names = instance.query(f"SELECT name FROM {TABLE_NAME} ORDER BY id").strip()

    # Verify initial state
    assert final_count == 3
    assert final_sum == 601.5
    assert final_names == "Alice\nBob\nCharlie"

    print(f"✓ Successfully tested table recreation for {TABLE_NAME}")
    print(f"  Initial count: {initial_count}, Final count: {final_count}")
    print(f"  Initial sum: {initial_sum}, Final sum: {final_sum}")
    print(f"  All queries work correctly after table recreation")