import glob
import os

from pyiceberg.table import StaticTable

from helpers.iceberg_utils import get_uuid_str, iceberg_local_interop_dir

# Same per-xdist-worker paths conftest uses (parallel-safe under --dist=each).
ICEBERG_DIR_NODE1 = iceberg_local_interop_dir("node1")
ICEBERG_DIR_NODE2 = iceberg_local_interop_dir("node2")


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
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
        """
    )
    node2.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '{ICEBERG_DIR_NODE2}/default/{TABLE_NAME}')
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
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
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


def test_spark_gzip_metadata_ch_write_spark_read(started_cluster_iceberg):
    """
    Spark creates an Iceberg table configured for gzip-compressed metadata,
    ClickHouse writes into it, then Spark reads back the new write.

    Regression for issue #109801: ClickHouse used to name the compressed
    metadata file `v{N}.gzip.metadata.json` (HTTP Content-Encoding token)
    instead of the Iceberg spec extension `v{N}.gz.metadata.json`, so Spark
    (Hadoop catalog) could not locate the metadata ClickHouse wrote.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_gzip_meta_ch_write_spark_read_" + get_uuid_str()

    # Spark creates the table with gzip metadata compression.
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2',
                'write.metadata.compression-codec' = 'gzip'
            );
        """
    )

    # ClickHouse points at the same location and writes, also using gzip metadata.
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}')
        """
    )
    ch_settings = {
        "allow_insert_into_iceberg": 1,
        "iceberg_metadata_compression_method": "gzip",
    }
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (42)", settings=ch_settings)
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (123)", settings=ch_settings)

    # ClickHouse reads its own writes.
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 2

    # The metadata ClickHouse wrote must use the spec `gz` extension, not `gzip`.
    listing = node1.exec_in_container(
        ["bash", "-c", f"ls {ICEBERG_DIR_NODE1}/default/{TABLE_NAME}/metadata"]
    )
    assert ".gz.metadata.json" in listing
    assert ".gzip.metadata.json" not in listing

    # Spark must be able to locate and read back the write ClickHouse made.
    spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")
    df = spark.sql(
        f"SELECT * FROM node1_catalog.default.{TABLE_NAME}"
    ).collect()
    spark_values = sorted([row.number for row in df])
    assert spark_values == [42, 123], f"Spark got unexpected values: {spark_values}"


def latest_metadata_file(table_dir):
    """Newest `vN.metadata.json` of a table, by `N`.

    The documents are numbered in write order, and two of them can carry the same
    `last-updated-ms`, so the number decides rather than the timestamp. That is also
    how ClickHouse itself resolves the table by default. `N` is not zero padded, so
    the name itself does not sort.
    """
    candidates = glob.glob(os.path.join(table_dir, "metadata", "v*.metadata.json"))
    assert candidates, f"no metadata file under {table_dir}"

    def version(path):
        return int(os.path.basename(path).split(".", 1)[0][1:])

    return max(candidates, key=version)


def test_ch_write_pyiceberg_read_bound_width(started_cluster_iceberg):
    """
    ClickHouse writes an Iceberg table, pyiceberg scans it with a row filter.

    Regression for issue #117072: a manifest bound is serialized with the width of
    its own Iceberg type (spec Appendix D), so `int` and `date` take 4 bytes and
    `long` takes 8. ClickHouse wrote the `int` and `date` bounds 8 bytes wide, and
    pyiceberg then raised `struct.error: unpack requires a buffer of 4 bytes` while
    planning any filtered scan of such a table. Unfiltered scans never read the
    bounds, so the breakage looked intermittent.
    """
    node1 = started_cluster_iceberg.instances["node1"]

    TABLE_NAME = "test_pyiceberg_bound_width_" + get_uuid_str()
    table_dir = f"{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}"
    ch_settings = {"allow_insert_into_iceberg": 1}

    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME} (i32 Int32, d Date, i64 Int64, s String, d32 Date32)
        ENGINE=IcebergLocal(local,
            path = '{table_dir}', format=Parquet)
        ORDER BY (i32)
        """,
        settings=ch_settings,
    )
    # One row, so each column's lower and upper bound hold the same value.
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (42, '2024-06-01', 100, 'abc', '1950-01-01')",
        settings=ch_settings,
    )
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 1

    # external_dirs mounts the container path at the same absolute path on the host,
    # so pyiceberg opens the very table ClickHouse just wrote.
    table = StaticTable.from_metadata(latest_metadata_file(table_dir))

    entries = [
        entry
        for manifest in table.current_snapshot().manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io)
    ]
    assert len(entries) == 1

    # Raw bytes, because a decoder reads its own width and returns the right number
    # from a bound of any length. Field 5 is negative (1950-01-01 is day -7305), so
    # it pins the sign as well as the width.
    expected_bounds = {
        1: "2A000000",  # i32, Iceberg `int`
        2: "A34D0000",  # d, `date`
        3: "6400000000000000",  # i64, `long`
        4: "616263",  # s, `string`
        5: "77E3FFFF",  # d32, `date` before the epoch
    }
    for bounds in (entries[0].data_file.lower_bounds, entries[0].data_file.upper_bounds):
        assert {
            field_id: value.hex().upper() for field_id, value in bounds.items()
        } == expected_bounds

    # pyiceberg reads those bounds when it plans a filtered scan. The `long` and
    # `string` filters and the unfiltered scan succeeded on the 8-byte bounds too,
    # so they show the failure was confined to the narrow types.
    for row_filter in [
        "i32 >= 0",
        "d >= '2024-01-01'",
        "d32 <= '1950-01-02'",
        "i64 >= 0",
        "s >= 'a'",
    ]:
        assert len(table.scan(row_filter=row_filter).to_arrow()) == 1, row_filter

    # The bounds are usable and not merely parseable: each of these excludes the row.
    for row_filter in ["i32 > 42", "d > '2024-06-01'", "d32 > '1950-01-01'"]:
        assert len(table.scan(row_filter=row_filter).to_arrow()) == 0, row_filter

    assert len(table.scan().to_arrow()) == 1
