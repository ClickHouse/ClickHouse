import pytest

from helpers.iceberg_utils import check_validity_and_get_prunned_files_general


@pytest.mark.parametrize(
    "storage_type",
    ["s3", "azure", "local"],
)
def test_partition_pruning_with_subquery_set(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_partition_pruning_{storage_type}")
    in_memory_table = engine.unique_table("in_memory_table")

    engine.create_table(
        table,
        [("id", "int"), ("data", "string")],
        partition_by="id",
        format_version=2,
    )
    engine.insert(
        table,
        [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")],
    )
    engine.sync(table, storage_type)
    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query(f"CREATE TABLE {in_memory_table} (id INT) ENGINE = Memory")
    node.query(f"INSERT INTO {in_memory_table} VALUES (2), (4)")

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {"use_iceberg_partition_pruning": 0}
        settings2 = {"use_iceberg_partition_pruning": 1}
        return check_validity_and_get_prunned_files_general(
            node,
            table,
            settings1,
            settings2,
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {target} WHERE id in (SELECT id FROM {in_memory_table}) ORDER BY ALL"
        )
        == 3
    )
