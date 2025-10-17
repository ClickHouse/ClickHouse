from helpers.iceberg_utils import get_uuid_str


def test_optimize_after_position_deletes_orc(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    table = f"iceberg_orc_posdel_{get_uuid_str()}"
    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table}/"

    # Create IcebergLocal table and insert a row
    instance.query(
        f"CREATE TABLE {table} (c0 Int) ENGINE = IcebergLocal('{path}')",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    instance.query(f"INSERT INTO {table} (c0) VALUES (1)")

    # Create equality delete
    instance.query(f"ALTER TABLE {table} DELETE WHERE c0 = 1")

    # Append a data file written as ORC using icebergLocal to reproduce missing row-number info
    instance.query(
        (
            "INSERT INTO TABLE FUNCTION icebergLocal("
            "local, structure = 'c0 Int', format = 'ORC', path = '{path}'"
            ") SELECT 2"
        ).format(path=path)
    )

    # This used to fail with: Logical error: 'ChunkInfoRowNumbers does not exist'
    instance.query(f"OPTIMIZE TABLE {table}")

    # Sanity check: data exists and is readable
    result = instance.query(f"SELECT groupArray(c0) FROM {table}")
    assert "2" in result
