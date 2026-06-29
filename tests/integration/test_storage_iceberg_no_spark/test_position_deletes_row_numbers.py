from helpers.iceberg_utils import get_uuid_str


def test_optimize_after_position_deletes_orc(started_cluster_iceberg_no_spark):
    """Regression test for https://github.com/ClickHouse/ClickHouse/issues/88123.

    Compaction (`OPTIMIZE`) converts equality deletes into position deletes and applies
    them while rewriting each data file. The position-delete transform maps physical row
    positions to a deletion bitmap using `ChunkInfoRowNumbers`. Parquet and row-based
    readers attach this info, but the ORC reader does not, so compacting a table that has
    an ORC data file used to fail with the exception
    `Logical error: 'ChunkInfoRowNumbers does not exist'`.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    table = f"iceberg_orc_posdel_{get_uuid_str()}"
    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table}/"

    # Create an IcebergLocal table and insert one row (written as Parquet).
    instance.query(f"CREATE TABLE {table} (c0 Int) ENGINE = IcebergLocal('{path}')")
    instance.query(f"INSERT INTO {table} (c0) VALUES (1)")

    # Delete the row. ClickHouse records this as an equality delete.
    instance.query(f"ALTER TABLE {table} DELETE WHERE c0 = 1")

    # Append a data file written as ORC via the icebergLocal table function.
    # The ORC reader does not attach ChunkInfoRowNumbers, which is what triggers the bug.
    instance.query(
        f"INSERT INTO TABLE FUNCTION icebergLocal("
        f"local, structure = 'c0 Int', format = 'ORC', path = '{path}') SELECT 2"
    )

    # Compaction reads the data files (including the ORC one) and applies position
    # deletes. This used to throw `ChunkInfoRowNumbers does not exist`.
    instance.query(
        f"OPTIMIZE TABLE {table}",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    # The deleted row must be gone and the appended row must remain.
    assert instance.query(f"SELECT groupArray(c0) FROM {table}").strip() == "[2]"
