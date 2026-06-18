import datetime

import pytest


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_types(engine, node, format_version, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_types_{format_version}_{storage_type}")
    engine.create_table(
        table,
        [("a", "int"), ("b", "string"), ("c", "date"), ("e", "bool")],
        format_version=format_version,
    )
    engine.insert(table, [(123, "string", datetime.date(2000, 1, 1), True)])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)
    assert int(node.query(f"SELECT count() FROM {target}").strip()) == 1
    assert (
        node.query(f"SELECT a, b, c, e FROM {target}").strip()
        == "123\tstring\t2000-01-01\ttrue"
    )

    desc = node.query(
        f"DESC {target} FORMAT TSV", settings={"print_pretty_type_names": 0}
    ).strip()
    types = {line.split("\t")[0]: line.split("\t")[1] for line in desc.splitlines()}
    assert types["a"] == "Nullable(Int32)"
    assert types["b"] == "Nullable(String)"
    assert types["c"] == "Nullable(Date32)"
    assert types["e"] == "Nullable(Bool)"
