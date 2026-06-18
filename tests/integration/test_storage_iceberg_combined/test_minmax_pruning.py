import datetime

import pytest

from helpers.iceberg_utils import check_validity_and_get_prunned_files_general


@pytest.mark.parametrize("storage_type", ["s3"])
def test_minmax_pruning(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_minmax_pruning_{storage_type}")
    engine.create_table(
        table,
        [("tag", "long"), ("dt", "date"), ("name", "string"), ("number", "long")],
        format_version=2,
    )
    for row in [
        (1, datetime.date(2024, 1, 20), "vasya", 5),
        (2, datetime.date(2024, 2, 20), "vasilisa", 6),
        (3, datetime.date(2025, 3, 20), "icebreaker", 7),
        (4, datetime.date(2025, 4, 20), "iceberg", 8),
    ]:
        engine.insert(table, [row])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    def pruned(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            node, table, settings1, settings2, "IcebergMinMaxIndexPrunedFiles", select_expression
        )

    assert pruned(f"SELECT * FROM {target} ORDER BY ALL") == 0
    assert pruned(f"SELECT * FROM {target} WHERE dt <= '2024-01-25' ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE tag == 1 ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE tag <= 1 ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE name == 'vasilisa' ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE name < 'kek' ORDER BY ALL") == 2
    assert pruned(f"SELECT * FROM {target} WHERE number == 8 ORDER BY ALL") == 3
    assert pruned(f"SELECT * FROM {target} WHERE number <= 5 ORDER BY ALL") == 3
