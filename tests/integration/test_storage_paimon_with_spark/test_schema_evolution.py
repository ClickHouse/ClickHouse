import pytest

from helpers.paimon_utils import (
    create_spark_paimon_table,
    execute_spark_query,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    spark_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_schema_evolution(started_cluster_paimon_with_spark, storage_type):
    """Schema evolution across snapshots. ClickHouse resolves Paimon schema
    evolution by column name against the latest snapshot's schema, so data
    files written before a RENAME expose NULL for the renamed column — this
    test pins that behavior."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_" + storage_type + "_" + get_uuid_str()

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )

    # Step 1: baseline.
    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, b STRING")
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["b", "Nullable(String)"]]
    assert data == [["1", "one"], ["2", "two"]]

    # Step 2: ADD COLUMN — old rows read NULL for the new column.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"ALTER TABLE {spark_table(table_name)} ADD COLUMN c BIGINT",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three', 30)",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [
        ["a", "Int32"],
        ["b", "Nullable(String)"],
        ["c", "Nullable(Int64)"],
    ]
    assert data == [["1", "one", "\\N"], ["2", "two", "\\N"], ["3", "three", "30"]]

    # Step 3: RENAME COLUMN — files written before the rename still store the
    # old column name, and name-based resolution reads NULL from them.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN b TO b2",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (4, 'four', 40)",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [
        ["a", "Int32"],
        ["b2", "Nullable(String)"],
        ["c", "Nullable(Int64)"],
    ]
    assert data == [
        ["1", "\\N", "\\N"],
        ["2", "\\N", "\\N"],
        ["3", "\\N", "30"],
        ["4", "four", "40"],
    ]

    # Step 4: DROP COLUMN.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"ALTER TABLE {spark_table(table_name)} DROP COLUMN a",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES ('five', 50)",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="c, b2")
    assert schema == [["b2", "Nullable(String)"], ["c", "Nullable(Int64)"]]
    assert data == [
        ["\\N", "30"],
        ["four", "40"],
        ["five", "50"],
        ["\\N", "\\N"],
        ["\\N", "\\N"],
    ]
