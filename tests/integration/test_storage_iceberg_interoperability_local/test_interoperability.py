from helpers.iceberg_utils import get_uuid_str

ICEBERG_DIR_NODE1 = "/var/lib/clickhouse/user_files/iceberg_node1"


def test_nodes_dont_see_each_other(started_cluster_iceberg):
    """
    Spark writes different data to each node's local directory.
    Each node only sees its own data.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    node2 = started_cluster_iceberg.instances["node2"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_isolation_" + get_uuid_str()

    # Create Iceberg tables via Spark — one per node catalog
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    spark.sql(
        f"""
            CREATE TABLE node2_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    # Write 100 rows to node1, 200 rows to node2
    spark.sql(
        f"""
            INSERT INTO node1_catalog.default.{TABLE_NAME}
            SELECT id as number FROM range(100)
        """
    )

    spark.sql(
        f"""
            INSERT INTO node2_catalog.default.{TABLE_NAME}
            SELECT id as number FROM range(200)
        """
    )

    # Create ClickHouse tables — each node reads from its own iceberg directory
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node1/default/{TABLE_NAME}')
        """
    )
    node2.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node2/default/{TABLE_NAME}')
        """
    )

    # Each node should only see its own data
    rows_node1 = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    rows_node2 = int(node2.query(f"SELECT count() FROM {TABLE_NAME}"))

    assert rows_node1 == 100, f"node1: expected 100 rows, got {rows_node1}"
    assert rows_node2 == 200, f"node2: expected 200 rows, got {rows_node2}"

    # Append more data to node1 only
    spark.sql(
        f"""
            INSERT INTO node1_catalog.default.{TABLE_NAME}
            SELECT id + 100 as number FROM range(50)
        """
    )

    rows_node1 = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    rows_node2 = int(node2.query(f"SELECT count() FROM {TABLE_NAME}"))

    assert rows_node1 == 150, f"node1: expected 150 rows after append, got {rows_node1}"
    assert rows_node2 == 200, f"node2: should still have 200 rows, got {rows_node2}"


def test_ch_write_spark_read(started_cluster_iceberg):
    """
    Spark creates a table, ClickHouse writes to it, Spark reads back.
    Validates that the external_dirs mount works bidirectionally.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_spark_read_" + get_uuid_str()

    # Spark creates the table structure
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    # Create ClickHouse table pointing to the same location
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node1/default/{TABLE_NAME}')
        """
    )

    # ClickHouse writes data
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (42)",
        settings={"allow_insert_into_iceberg": 1},
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (123)",
        settings={"allow_insert_into_iceberg": 1},
    )

    # ClickHouse can read its own writes
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 2

    # Spark should also see the data written by ClickHouse.
    # Spark's catalog caches metadata, so we need to refresh it first.
    spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")

    df = spark.sql(
        f"SELECT * FROM node1_catalog.default.{TABLE_NAME}"
    ).collect()
    assert len(df) == 2, f"Spark expected 2 rows, got {len(df)}"

    spark_values = sorted([row.number for row in df])
    assert spark_values == [42, 123], f"Spark got unexpected values: {spark_values}"


def test_spark_write_ch_read_append(started_cluster_iceberg):
    """Spark writes, CH reads, Spark appends, CH reads updated data."""
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_append_" + get_uuid_str()

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(
        f"INSERT INTO node1_catalog.default.{TABLE_NAME} SELECT id as number FROM range(100)"
    )

    # Create ClickHouse table pointing to the same location
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
        """
    )

    # CH reads Spark's data
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    result = node1.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    assert int(result) == 4950, f"Expected sum 4950, got {result.strip()}"

    # Spark appends more data
    spark.sql(
        f"INSERT INTO node1_catalog.default.{TABLE_NAME} SELECT id + 100 as number FROM range(50)"
    )

    # CH reads the updated data
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 150, f"Expected 150 rows after append, got {rows}"


def test_spark_delete_ch_read(started_cluster_iceberg):
    """Spark creates a table, inserts data, deletes some rows, and CH sees the deletions."""
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_delete_" + get_uuid_str()

    # Spark creates the table with merge-on-read delete mode (position deletes)
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
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
    spark.sql(
        f"INSERT INTO node1_catalog.default.{TABLE_NAME} SELECT id as number FROM range(100)"
    )

    # Create ClickHouse table
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
        """
    )

    # CH reads all 100 rows
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    # Spark deletes rows where number < 20
    spark.sql(f"DELETE FROM node1_catalog.default.{TABLE_NAME} WHERE number < 20")

    # CH should see only 80 rows
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 80, f"Expected 80 rows after first delete, got {rows}"

    result = int(node1.query(f"SELECT min(number) FROM {TABLE_NAME}"))
    assert result == 20, f"Expected min 20 after delete, got {result}"

    # Spark deletes more rows
    spark.sql(f"DELETE FROM node1_catalog.default.{TABLE_NAME} WHERE number >= 90")

    # CH should see only 70 rows (20..89)
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 70, f"Expected 70 rows after second delete, got {rows}"

    result = int(node1.query(f"SELECT max(number) FROM {TABLE_NAME}"))
    assert result == 89, f"Expected max 89 after delete, got {result}"

    result = int(node1.query(f"SELECT sum(number) FROM {TABLE_NAME}"))
    expected_sum = sum(range(20, 90))
    assert result == expected_sum, f"Expected sum {expected_sum}, got {result}"


def test_ch_delete_spark_read(started_cluster_iceberg):
    """Spark creates a table, CH deletes some rows, and Spark sees the deletions."""
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_delete_" + get_uuid_str()

    # Spark creates the table and inserts data
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(
        f"INSERT INTO node1_catalog.default.{TABLE_NAME} SELECT id as number FROM range(50)"
    )

    # Create ClickHouse table
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
        """
    )

    # CH reads all 50 rows
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 50, f"Expected 50 rows, got {rows}"

    # CH deletes some rows
    delete_settings = {"allow_insert_into_iceberg": 1}
    node1.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE number < 10",
        settings=delete_settings,
    )

    # CH sees the deletion
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 40, f"Expected 40 rows after CH delete, got {rows}"

    # Spark should also see the deletion
    spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")
    df = spark.sql(f"SELECT * FROM node1_catalog.default.{TABLE_NAME}").collect()
    assert len(df) == 40, f"Spark expected 40 rows after CH delete, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert min(spark_values) == 10, f"Spark expected min 10, got {min(spark_values)}"

    # CH deletes more rows
    node1.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE number >= 40",
        settings=delete_settings,
    )

    # CH sees the deletion
    rows = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 30, f"Expected 30 rows after second CH delete, got {rows}"

    # Spark should also see the second deletion
    spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")
    df = spark.sql(f"SELECT * FROM node1_catalog.default.{TABLE_NAME}").collect()
    assert len(df) == 30, f"Spark expected 30 rows after second CH delete, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert spark_values == list(range(10, 40)), \
        f"Spark expected values 10..39, got {spark_values}"
