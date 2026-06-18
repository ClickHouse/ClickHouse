import datetime

import pytest

from helpers.iceberg_engine import Struct
from helpers.iceberg_utils import check_validity_and_get_prunned_files_general

_TS_FIELDS = [("a", "date"), ("b", "timestamp")]


@pytest.mark.parametrize("storage_type", ["s3"])
def test_minmax_pruning_with_null(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_minmax_pruning_with_null_{storage_type}")
    engine.create_table(
        table,
        [
            ("tag", "int"),
            ("dt", "date"),
            ("ts", "timestamp"),
            ("time_struct", "struct(a:date,b:timestamp)"),
            ("name", "string"),
            ("number", "long"),
        ],
        format_version=2,
    )

    null_struct = Struct(_TS_FIELDS, None)
    rows = [
        (1, datetime.date(2024, 1, 20), datetime.datetime(2024, 2, 20, 10, 0, 0), null_struct, "vasya", 5),
        (2, datetime.date(2024, 2, 20), datetime.datetime(2024, 3, 20, 15, 0, 0), null_struct, "vasilisa", 6),
        (3, datetime.date(2025, 3, 20), datetime.datetime(2024, 4, 30, 14, 0, 0), null_struct, "icebreaker", 7),
        (4, datetime.date(2025, 4, 20), datetime.datetime(2024, 5, 30, 14, 0, 0), null_struct, "iceberg", 8),
        (1, datetime.date(2024, 1, 20), datetime.datetime(2024, 2, 20, 10, 0, 0),
         Struct(_TS_FIELDS, [datetime.date(2024, 2, 20), datetime.datetime(2024, 2, 20, 10, 0, 0)]), "vasya", 5),
    ]
    for row in rows:
        engine.insert(table, [row])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    def pruned(select_expression):
        return check_validity_and_get_prunned_files_general(
            node, table,
            {"use_iceberg_partition_pruning": 0, "input_format_parquet_bloom_filter_push_down": 0, "input_format_parquet_filter_push_down": 0},
            {"use_iceberg_partition_pruning": 1, "input_format_parquet_bloom_filter_push_down": 0, "input_format_parquet_filter_push_down": 0},
            "IcebergMinMaxIndexPrunedFiles",
            select_expression,
        )

    assert pruned(f"SELECT * FROM {target} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL") == 1
