import glob
import json
import os

import pytest
from pyiceberg.table import StaticTable

from helpers.iceberg_utils import get_uuid_str, iceberg_local_interop_dir

ICEBERG_DIR_NODE1 = iceberg_local_interop_dir("node1")

CH_SETTINGS = {"allow_insert_into_iceberg": 1}


def latest_metadata_file(table_dir):
    candidates = glob.glob(os.path.join(table_dir, "metadata", "v*.metadata.json"))
    assert candidates, f"no metadata file under {table_dir}"
    return max(candidates, key=lambda path: int(os.path.basename(path).split(".", 1)[0][1:]))


def test_ch_write_truncate_int_partition_spark_pyiceberg_read(started_cluster_iceberg):
    """
    Spark creates a table partitioned by `truncate(10, x)` of an `int` column, ClickHouse writes, deletes
    and updates rows, and Spark and pyiceberg read the result.

    Regression for issue #118905: ClickHouse declared that partition field `long`, with 8-byte bounds.
    pyiceberg failed every filtered scan (`unpack requires a buffer of 4 bytes`), and Spark did not pair
    the partition-scoped position deletes of a ClickHouse DELETE or UPDATE with their data files, so
    deleted rows came back.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_truncate_int_partition_" + get_uuid_str()
    table_dir = f"{ICEBERG_DIR_NODE1}/default/{TABLE_NAME}"

    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (x INT, v STRING)
            USING iceberg
            PARTITIONED BY (truncate(10, x))
            OPTIONS('format-version'='2');
        """
    )
    node1.query(
        f"CREATE TABLE {TABLE_NAME} ENGINE=IcebergLocal(local, path = '{table_dir}')"
    )

    # Two INSERTs of the same rows, so every partition has two data files and each
    # position delete file of a mutation covers both of them.
    values = ", ".join(f"({x}, 'a')" for x in [-12, -5, -1, 0, 3, 5, 9, 10, 12, 21])
    for _ in range(2):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES {values}", settings=CH_SETTINGS)
    # Iceberg Java stores `truncate[10](-2147483648)` as the 32-bit wraparound 2147483646.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (-2147483648, 'min')", settings=CH_SETTINGS)

    def ch_rows():
        result = node1.query(f"SELECT x, v FROM {TABLE_NAME} ORDER BY x, v FORMAT TSV")
        return [(int(x), v) for x, v in (line.split("\t") for line in result.splitlines())]

    def spark_rows(where=""):
        spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")
        df = spark.sql(f"SELECT x, v FROM node1_catalog.default.{TABLE_NAME} {where}").collect()
        return sorted((row.x, row.v) for row in df)

    assert len(ch_rows()) == 21
    assert spark_rows() == ch_rows()
    assert spark_rows("WHERE x = -2147483648") == [(-2147483648, "min")]
    assert spark_rows("WHERE x = 5") == [(5, "a"), (5, "a")]

    table = StaticTable.from_metadata(latest_metadata_file(table_dir))
    assert table.scan(row_filter="x == 5").to_arrow().num_rows == 2
    assert table.scan(row_filter="x >= 10").to_arrow().num_rows == 6

    node1.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x % 2 = 0", settings=CH_SETTINGS)
    assert len(ch_rows()) == 12
    assert spark_rows() == ch_rows()

    node1.query(f"ALTER TABLE {TABLE_NAME} UPDATE v = 'u' WHERE x % 3 = 0", settings=CH_SETTINGS)
    assert sorted(set(ch_rows())) == [(-5, "a"), (-1, "a"), (3, "u"), (5, "a"), (9, "u"), (21, "u")]
    assert len(ch_rows()) == 12
    assert spark_rows() == ch_rows()

    table = StaticTable.from_metadata(latest_metadata_file(table_dir))
    assert table.scan(row_filter="x == 5").to_arrow().num_rows == 2


@pytest.mark.parametrize("expire_int_schema", [False, True])
def test_mutations_refused_after_promoting_wide_truncate_int(started_cluster_iceberg, expire_int_schema):
    """
    Iceberg Java computes `truncate[1500000000](1400000000)` of an `int` with a 32-bit overflow as -1500000000, a
    multiple of the width that the same value of a `long` truncates to 0. After the column is promoted to `long`,
    ClickHouse refuses DELETE and UPDATE instead of writing position deletes that never pair with the data file,
    also when snapshot expiry has removed every `int` schema from the table metadata.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    def create_promoted_table(values):
        table_name = "test_truncate_wide_int_promoted_" + get_uuid_str()
        table_dir = f"{ICEBERG_DIR_NODE1}/default/{table_name}"
        spark.sql(
            f"""
                CREATE TABLE node1_catalog.default.{table_name} (id INT, x INT)
                USING iceberg
                PARTITIONED BY (truncate(1500000000, x))
                OPTIONS('format-version'='2');
            """
        )
        spark.sql(f"INSERT INTO node1_catalog.default.{table_name} VALUES {values}")
        keys = spark.sql(f"SELECT partition.x_trunc FROM node1_catalog.default.{table_name}.files").collect()

        if expire_int_schema:
            spark.sql(f"ALTER TABLE node1_catalog.default.{table_name} ALTER COLUMN x TYPE BIGINT")
            spark.sql(f"INSERT INTO node1_catalog.default.{table_name} VALUES (3, 7)")
            spark.sql(
                f"""
                    CALL node1_catalog.system.expire_snapshots(
                        table => 'default.{table_name}',
                        older_than => TIMESTAMP '2100-01-01 00:00:00',
                        retain_last => 1,
                        clean_expired_metadata => true)
                """
            )
            with open(latest_metadata_file(table_dir)) as f:
                schemas = json.load(f)["schemas"]
            assert [field["type"] for schema in schemas for field in schema["fields"] if field["name"] == "x"] == ["long"]

        node1.query(
            f"CREATE TABLE {table_name} ENGINE=IcebergLocal(local, path = '{table_dir}')"
        )
        if not expire_int_schema:
            node1.query(f"ALTER TABLE {table_name} MODIFY COLUMN x Nullable(Int64)", settings=CH_SETTINGS)
        return table_name, sorted(row.x_trunc for row in keys)

    appended_row = "3\t7\n" if expire_int_schema else ""

    table_name, keys = create_promoted_table("(1, 1400000000), (2, 5)")
    assert keys == [-1500000000, 0]
    for mutation in ["DELETE WHERE id = 1", "UPDATE id = 3 WHERE id = 1"]:
        error = node1.query_and_get_error(f"ALTER TABLE {table_name} {mutation}", settings=CH_SETTINGS)
        assert "NOT_IMPLEMENTED" in error
    assert node1.query(f"SELECT id, x FROM {table_name} ORDER BY id") == "1\t1400000000\n2\t5\n" + appended_row

    # The `long` transform gives the recorded key for every value between the bounds of this file.
    table_name, keys = create_promoted_table("(2, 5)")
    assert keys == [0]
    node1.query(f"ALTER TABLE {table_name} DELETE WHERE id = 2", settings=CH_SETTINGS)
    assert node1.query(f"SELECT id, x FROM {table_name} ORDER BY id") == appended_row
