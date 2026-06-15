import pytest


@pytest.mark.parametrize("storage_type", ["s3"])
def test_update(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_update_{storage_type}")
    engine.create_table(table, [("id", "long"), ("name", "string")], format_version=2)
    engine.insert(table, [(1, "old"), (2, "keep"), (3, "old")])
    engine.update(table, {"name": "new"}, "name = 'old'")
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    result = node.query(
        f"SELECT id, name FROM {target} ORDER BY id FORMAT TSV"
    ).strip()
    assert result == "1\tnew\n2\tkeep\n3\tnew"
