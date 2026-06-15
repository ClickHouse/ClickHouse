import datetime

import pytest

from helpers.iceberg_utils import check_validity_and_get_prunned_files_general


@pytest.mark.parametrize("storage_type", ["s3"])
def test_bucket_partition_pruning(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_bucket_partition_pruning_{storage_type}")
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
            ("bucket", 4, "value"),
            ("bucket", 5, "created_at"),
            ("bucket", 3, "event_time"),
        ],
        format_version=2,
    )
    engine.insert(table, [
        (1, "Alice", 10.50, datetime.date(2024, 1, 20), datetime.datetime(2024, 1, 20, 10, 0, 0)),
        (2, "Bob", 20.00, datetime.date(2024, 1, 21), datetime.datetime(2024, 1, 21, 11, 0, 0)),
        (3, "Charlie", 30.50, datetime.date(2024, 1, 22), datetime.datetime(2024, 1, 22, 12, 0, 0)),
        (4, "Diana", 40.00, datetime.date(2024, 1, 23), datetime.datetime(2024, 1, 23, 13, 0, 0)),
        (5, "Eve", 50.50, datetime.date(2024, 1, 24), datetime.datetime(2024, 1, 24, 14, 0, 0)),
    ])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    def pruned(select_expression):
        return check_validity_and_get_prunned_files_general(
            node, table,
            {"use_iceberg_partition_pruning": 0},
            {"use_iceberg_partition_pruning": 1},
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    queries = [
        f"SELECT * FROM {target} WHERE id == 1 ORDER BY ALL",
        f"SELECT * FROM {target} WHERE value == 20.00 OR event_time == '2024-01-24 14:00:00' ORDER BY ALL",
        f"SELECT * FROM {target} WHERE id == 3 AND name == 'Charlie' ORDER BY ALL",
        f"SELECT * FROM {target} WHERE (event_time == '2024-01-21 11:00:00' AND name == 'Bob') OR (name == 'Eve' AND id == 5) ORDER BY ALL",
    ]
    for query in queries:
        assert pruned(query) > 0
