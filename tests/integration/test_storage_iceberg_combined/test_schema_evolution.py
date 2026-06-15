import pytest


@pytest.mark.parametrize("storage_type", ["s3"])
def test_add_column(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_add_column_{storage_type}")
    engine.create_table(table, [("id", "long"), ("val", "string")], format_version=2)
    engine.insert(table, [(1, "a"), (2, "b")])
    engine.add_column(table, "extra", "long")
    engine.insert(table, [(3, "c", 100), (4, "d", 200)])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    result = node.query(
        f"SELECT id, val, extra FROM {target} ORDER BY id FORMAT TSV"
    ).strip()
    assert result == "1\ta\t\\N\n2\tb\t\\N\n3\tc\t100\n4\td\t200"
