from datetime import datetime
import time
import pytest

from helpers.iceberg_utils import check_schema_and_data


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_schema_evolution_with_time_travel(engine, node, format_version, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    first_timestamp_ms = int(datetime.now().timestamp() * 1000)
    time.sleep(0.5)

    table = engine.unique_table(
        f"test_schema_evolution_with_time_travel_{format_version}_{storage_type}"
    )
    engine.create_table(table, [("a", "int")], format_version=format_version)
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"]
        ],
        [],
    )

    engine.insert(table, [(4,)])
    engine.sync(table, storage_type)

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
        ],
        [["4"]],
    )

    error_message = node.query_and_get_error(f"SELECT * FROM {target} ORDER BY ALL SETTINGS iceberg_timestamp_ms = {first_timestamp_ms}")
    assert "No snapshot found in snapshot log before requested timestamp" in error_message

    second_timestamp_ms = int(datetime.now().timestamp() * 1000)

    time.sleep(0.5)

    engine.add_column(table, "b", "double")
    engine.sync(table, storage_type)

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(Float64)"]
        ],
        [["4", "\\N"]],
    )

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
        ],
        [["4"]],
        timestamp_ms=second_timestamp_ms,
    )

    third_timestamp_ms = int(datetime.now().timestamp() * 1000)

    time.sleep(0.5)

    engine.insert(table, [(7, 5.0)])
    engine.sync(table, storage_type)

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(Float64)"]
        ],
        [["4", "\\N"], ["7", "5"]],
    )

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
        ],
        [["4"]],
        timestamp_ms=second_timestamp_ms,
    )

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
        ],
        [["4"]],
        timestamp_ms=third_timestamp_ms,
    )

    engine.add_column(table, "c", "double")
    engine.sync(table, storage_type)

    time.sleep(0.5)
    fourth_timestamp_ms = int(datetime.now().timestamp() * 1000)

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(Float64)"]
        ],
        [["4", "\\N"], ["7", "5"]],
        timestamp_ms=fourth_timestamp_ms,
    )

    check_schema_and_data(
        node,
        target,
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(Float64)"],
            ["c", "Nullable(Float64)"]
        ],
        [["4", "\\N", "\\N"], ["7", "5", "\\N"]],
    )
