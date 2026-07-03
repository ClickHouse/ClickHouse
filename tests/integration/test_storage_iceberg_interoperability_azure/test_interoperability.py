import logging

from helpers.iceberg_utils import get_uuid_str

AZURE_CONTAINER = "testcontainer"

def test_spark_write_ch_read_append(started_cluster_iceberg):
    """Spark writes, CH reads, Spark appends, CH reads updated data."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_append_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(100)")

    # Create ClickHouse table pointing to the same Azurite location
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        """
    )

    # CH reads Spark's data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    result = instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    assert int(result) == 4950, f"Expected sum 4950, got {result.strip()}"

    # Spark appends more data
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id + 100 as number FROM range(50)")

    # CH reads the updated data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 150, f"Expected 150 rows after append, got {rows}"


def test_ch_write_spark_read(started_cluster_iceberg):
    """ClickHouse writes to an Iceberg table on Azurite that Spark created.
    Tests that CH can correctly resolve Spark's wasb:// metadata paths and
    append new data while preserving the existing Spark-written data.
    Verifies Spark can read back via both SQL catalog and wasb:// path."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(10)")

    # Create ClickHouse table pointing to the same Azurite location.
    # iceberg_use_version_hint writes version-hint.text so Spark's HadoopCatalog
    # can discover the latest metadata version after session restart.
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        SETTINGS iceberg_use_version_hint = 1
        """
    )

    # CH reads Spark's data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 10, f"Expected 10 rows from Spark, got {rows}"

    # ClickHouse writes more data.
    # write_full_path_in_iceberg_metadata is needed so Spark can resolve the
    # data file paths (Spark expects wasb:// URIs, not relative paths).
    insert_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (42)", settings=insert_settings)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123)", settings=insert_settings)

    # ClickHouse can read its own writes (10 from Spark + 2 from CH)
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 12, f"Expected 12 rows, got {rows}"

    result = instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    expected_sum = sum(range(10)) + 42 + 123  # 45 + 42 + 123 = 210
    assert int(result) == expected_sum, f"Expected sum {expected_sum}, got {result.strip()}"

    # Spark should also see the data written by ClickHouse.
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    # Read via SQL catalog
    df = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()
    assert len(df) == 12, f"Spark SQL expected 12 rows, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert 42 in spark_values, f"Spark SQL missing CH-written value 42: {spark_values}"
    assert 123 in spark_values, f"Spark SQL missing CH-written value 123: {spark_values}"


def test_spark_delete_ch_read(started_cluster_iceberg):
    """Spark creates a table, inserts data, deletes some rows, and CH sees the deletions."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_delete_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table with merge-on-read delete mode (position deletes)
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2',
                'write.update.mode' = 'merge-on-read',
                'write.delete.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            );
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(100)")

    # Create ClickHouse table
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        """
    )

    # CH reads all 100 rows
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    # Spark deletes rows where number < 20
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE number < 20")

    # CH should see only 80 rows
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 80, f"Expected 80 rows after first delete, got {rows}"

    result = int(instance.query(f"SELECT min(number) FROM {TABLE_NAME}"))
    assert result == 20, f"Expected min 20 after delete, got {result}"

    # Spark deletes more rows
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE number >= 90")

    # CH should see only 70 rows (20..89)
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 70, f"Expected 70 rows after second delete, got {rows}"

    result = int(instance.query(f"SELECT max(number) FROM {TABLE_NAME}"))
    assert result == 89, f"Expected max 89 after delete, got {result}"

    result = int(instance.query(f"SELECT sum(number) FROM {TABLE_NAME}"))
    expected_sum = sum(range(20, 90))
    assert result == expected_sum, f"Expected sum {expected_sum}, got {result}"


def test_ch_delete_spark_read(started_cluster_iceberg):
    """Spark creates a table, CH deletes some rows, and Spark sees the deletions."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_delete_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(50)")

    # Create ClickHouse table with version hint so Spark can discover CH's metadata updates
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        SETTINGS iceberg_use_version_hint = 1
        """
    )

    # CH reads all 50 rows
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 50, f"Expected 50 rows, got {rows}"

    # CH deletes some rows
    delete_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }
    instance.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE number < 10",
        settings=delete_settings,
    )

    # CH sees the deletion
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 40, f"Expected 40 rows after CH delete, got {rows}"

    # Spark should also see the deletion
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    df = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()
    assert len(df) == 40, f"Spark expected 40 rows after CH delete, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert min(spark_values) == 10, f"Spark expected min 10, got {min(spark_values)}"

    # CH deletes more rows
    instance.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE number >= 40",
        settings=delete_settings,
    )

    # CH sees the deletion
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 30, f"Expected 30 rows after second CH delete, got {rows}"

    # Spark should also see the second deletion
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    df = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()
    assert len(df) == 30, f"Spark expected 30 rows after second CH delete, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert spark_values == list(range(10, 40)), \
        f"Spark expected values 10..39, got {spark_values}"