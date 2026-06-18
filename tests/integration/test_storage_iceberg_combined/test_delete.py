import uuid

import pytest


def _count_with_trivial_check(engine, node, target, expected):
    query_id = "test_delete_" + uuid.uuid4().hex
    count = int(node.query(f"SELECT count() FROM {target}", query_id=query_id).strip())
    assert count == expected
    if engine.name == "spark":
        node.query("SYSTEM FLUSH LOGS")
        applied = node.query(
            "SELECT ProfileEvents['IcebergTrivialCountOptimizationApplied'] "
            f"FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).strip()
        assert applied == "1"


@pytest.mark.parametrize("storage_type", ["s3"])
def test_delete(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_delete_{storage_type}")
    engine.create_table(table, [("a", "long"), ("b", "string")], format_version=2)
    engine.insert(table, [(i, str(i + 1)) for i in range(100)])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    _count_with_trivial_check(engine, node, target, 100)

    engine.delete(table, "a >= 50")
    engine.sync(table, storage_type)
    _count_with_trivial_check(engine, node, target, 50)

    engine.delete(table, "a >= 0")
    engine.sync(table, storage_type)
    _count_with_trivial_check(engine, node, target, 0)
