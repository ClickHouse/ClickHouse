import pytest


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_partition_by(engine, node, format_version, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_partition_by_{format_version}_{storage_type}")
    engine.create_table(
        table, [("a", "long"), ("b", "string")],
        partition_by="a", format_version=format_version,
    )
    engine.insert(table, [(i, str(i + 1)) for i in range(10)])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    assert int(node.query(f"SELECT count() FROM {target}").strip()) == 10
    assert int(node.query(
        f"SELECT count() FROM system.iceberg_history WHERE table LIKE '%{table}%'"
    ).strip()) >= 1
