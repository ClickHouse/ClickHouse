import datetime

import pytest

from helpers.iceberg_utils import check_validity_and_get_prunned_files_general


@pytest.mark.parametrize("storage_type", ["s3"])
def test_partition_pruning(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_partition_pruning_{storage_type}")
    engine.create_table(
        table,
        [
            ("tag", "int"),
            ("date", "date"),
            ("date2", "date"),
            ("ts", "timestamp"),
            ("ts2", "timestamp"),
            ("name", "string"),
            ("number", "long"),
        ],
        partition_by=[
            ("identity", "tag"),
            ("day", "date"),
            ("year", "date2"),
            ("hour", "ts"),
            ("month", "ts2"),
            ("truncate", 3, "name"),
            ("truncate", 3, "number"),
        ],
        format_version=2,
    )

    engine.insert(table, [
        (1, datetime.date(2024, 1, 20), datetime.date(2024, 1, 20),
         datetime.datetime(2024, 2, 20, 10, 0, 0), datetime.datetime(2024, 2, 20, 10, 0, 0), "vasya", 5),
        (2, datetime.date(2024, 1, 30), datetime.date(2024, 1, 30),
         datetime.datetime(2024, 3, 20, 15, 0, 0), datetime.datetime(2024, 3, 20, 15, 0, 0), "vasilisa", 6),
        (1, datetime.date(2024, 2, 20), datetime.date(2024, 2, 20),
         datetime.datetime(2024, 3, 20, 20, 0, 0), datetime.datetime(2024, 3, 20, 20, 0, 0), "iceberg", 7),
        (2, datetime.date(2025, 1, 20), datetime.date(2025, 1, 20),
         datetime.datetime(2024, 4, 30, 14, 0, 0), datetime.datetime(2024, 4, 30, 14, 0, 0), "icebreaker", 8),
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

    assert pruned(f"SELECT * FROM {target} ORDER BY ALL") == 0
    assert pruned(f"SELECT * FROM {target} WHERE date <= '2024-01-25' ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE date2 <= '2024-01-25' ORDER BY ALL") == 1
    assert pruned(f"SELECT * FROM {target} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE ts2 <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL") == 1
    assert pruned(f"SELECT * FROM {target} WHERE tag == 1 ORDER BY ALL") == 2
    assert pruned(f"SELECT * FROM {target} WHERE tag <= 1 ORDER BY ALL") == 2
    assert pruned(f"SELECT * FROM {target} WHERE name == 'vasilisa' ORDER BY ALL") == 2
    assert pruned(f"SELECT * FROM {target} WHERE name < 'kek' ORDER BY ALL") == 2
    assert pruned(f"SELECT * FROM {target} WHERE number == 8 ORDER BY ALL") == 1
    assert pruned(f"SELECT * FROM {target} WHERE number <= 5 ORDER BY ALL") == 3
