import pytest


@pytest.mark.parametrize("format_version", [1, 2])
def test_pruning_nullable_bug(engine, node, format_version):
    table = engine.unique_table(f"test_pruning_nullable_bug_{format_version}")
    engine.create_table(table, [("c0", "string")], format_version=format_version)
    engine.insert(table, [("Pasha Technick",), (None,)])
    engine.sync(table)

    target = engine.clickhouse_read_target(node, table)
    assert int(node.query(f"SELECT count() FROM {target} WHERE c0 IS NULL").strip()) == 1
    assert node.query(f"SELECT * FROM {target} WHERE c0 IS NULL") == "\\N\n"
