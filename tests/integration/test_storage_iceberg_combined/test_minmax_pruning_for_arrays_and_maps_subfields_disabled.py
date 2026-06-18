import pytest


@pytest.mark.parametrize("storage_type", ["s3"])
def test_minmax_pruning_for_arrays_and_maps_subfields_disabled(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(
        f"test_disable_minmax_pruning_for_arrays_and_maps_subfields_{storage_type}"
    )
    engine.create_table(
        table,
        [("id", "long"), ("measurements", "array(double)")],
        format_version=2,
    )
    engine.insert(
        table,
        [
            (1, [23.5, 24.1, 22.8, 25.3, 23.9]),
            (2, [18.2, 19.5, 17.8, 20.1, 19.3, 18.7]),
            (3, [30.0, 31.2, 29.8, 32.1, 30.5, 29.9, 31.0]),
            (4, [15.5, 16.2, 14.8, 17.1, 16.5]),
            (5, [27.3, 28.1, 26.9, 29.2, 28.5, 27.8, 28.3, 27.6]),
        ],
    )
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query(f"SELECT * FROM {target} ORDER BY ALL")
