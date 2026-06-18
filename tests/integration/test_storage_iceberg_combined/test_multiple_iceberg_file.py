import pytest


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_multiple_iceberg_files(engine, node, format_version, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_multiple_iceberg_files_{format_version}_{storage_type}")
    engine.create_table(table, [("a", "long"), ("b", "string")], format_version=format_version)
    engine.insert(table, [(i, str(i + 1)) for i in range(100)])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    assert int(node.query(f"SELECT count() FROM {target}").strip()) == 100

    engine.insert(table, [(i, str(i + 1)) for i in range(100, 200)])
    engine.sync(table, storage_type)

    assert int(node.query(f"SELECT count() FROM {target}").strip()) == 200
    assert node.query(f"SELECT * FROM {target} ORDER BY 1") == node.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )
