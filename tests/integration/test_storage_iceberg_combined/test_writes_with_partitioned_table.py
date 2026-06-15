import pytest


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_with_partitioned_table(engine, node, format_version, storage_type):
    table = engine.unique_table(f"test_writes_with_partitioned_table_{storage_type}")
    engine.create_table(
        table,
        [
            ("id", "int"),
            ("name", "string"),
            ("value", "decimal(10, 2)"),
            ("created_at", "date"),
            ("event_time", "timestamp"),
        ],
        partition_by=[
            ("bucket", 3, "id"),
            ("bucket", 2, "name"),
            ("bucket", 5, "created_at"),
            ("bucket", 3, "event_time"),
        ],
        format_version=format_version,
    )
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query(
        f"""
        INSERT INTO {target} VALUES
        (1, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (2, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (3, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (4, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (5, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    assert node.query(f"SELECT * FROM {target} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'

    node.query(
        f"""
        INSERT INTO {target} VALUES
        (10, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (20, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (30, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (40, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (50, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    assert node.query(f"SELECT * FROM {target} ORDER BY ALL") == '1\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n2\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n3\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n4\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n5\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n10\tAlice\t10.5\t2024-01-20\t2024-01-20 10:00:00.000000\n20\tBob\t20\t2024-01-21\t2024-01-21 11:00:00.000000\n30\tCharlie\t30.5\t2024-01-22\t2024-01-22 12:00:00.000000\n40\tDiana\t40\t2024-01-23\t2024-01-23 13:00:00.000000\n50\tEve\t50.5\t2024-01-24\t2024-01-24 14:00:00.000000\n'


@pytest.mark.parametrize("storage_type", ["s3"])
@pytest.mark.parametrize("partition_transform", ["year", "month", "day"])
def test_writes_spark_date_partition_by_time_transform(engine, node, storage_type, partition_transform):
    table = engine.unique_table(f"test_date_partition_{partition_transform}_{storage_type}")
    engine.create_table(
        table,
        [("c0", "date")],
        partition_by=[(partition_transform, "c0")],
        format_version=2,
    )
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query(
        f"INSERT INTO {target} (c0) VALUES ('2025-08-28');",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert node.query(f"SELECT * FROM {target} ORDER BY ALL") == '2025-08-28\n'
