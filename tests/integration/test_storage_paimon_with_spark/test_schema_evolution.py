import pytest

from helpers.paimon_utils import (
    PAIMON_CATALOG,
    create_incremental_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    execute_spark_query,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    spark_table,
    wait_until_query_result,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_schema_evolution(started_cluster_paimon_with_spark, storage_type):
    """ADD COLUMN and DROP COLUMN across snapshots: files written with an older
    schema are read by column name, which is correct while no field id was
    renamed and no name was given to another field id."""
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

    # Step 3: DROP COLUMN. Files written before the drop still hold column a, which is not read.
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
        f"INSERT INTO {spark_table(table_name)} VALUES ('four', 40)",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="c, b")
    assert schema == [["b", "Nullable(String)"], ["c", "Nullable(Int64)"]]
    assert data == [["three", "30"], ["four", "40"], ["one", "\\N"], ["two", "\\N"]]


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_schema_evolution_rename_column(
    started_cluster_paimon_with_spark, storage_type
):
    """Files written before a RENAME COLUMN store the field under its old name,
    so reading them by the new name would return NULL: the read must fail."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_rename_" + storage_type + "_" + get_uuid_str()

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )

    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, b STRING")
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')",
    )
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
        f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')",
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_expr}")
    assert "NOT_IMPLEMENTED" in error
    assert "field id 1 is column 'b' in schema 0" in error
    assert "but column 'b2' in the current schema 1" in error
    assert "INSERT OVERWRITE" in error

    # Rewriting the table with the current schema makes the old rows readable under the new name.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT OVERWRITE {spark_table(table_name)} SELECT * FROM {spark_table(table_name)}",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["b2", "Nullable(String)"]]
    assert data == [["1", "one"], ["2", "two"], ["3", "three"]]


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_schema_evolution_drop_and_add_same_name(
    started_cluster_paimon_with_spark, storage_type
):
    """After DROP COLUMN email + ADD COLUMN email, files written before the drop
    hold the dropped field under the same name, so reading them by name would
    return the dropped values: the read must fail."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_readd_" + storage_type + "_" + get_uuid_str()

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )

    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, email STRING")
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES "
        "(1, 'alice@example.com'), (2, 'bob@example.com')",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"ALTER TABLE {spark_table(table_name)} DROP COLUMN email",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"ALTER TABLE {spark_table(table_name)} ADD COLUMN email STRING",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (3, 'carol@example.com')",
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_expr}")
    assert "NOT_IMPLEMENTED" in error
    assert "column 'email' is field id 1 in schema 0" in error
    assert "but field id 2 in the current schema 2" in error
    assert "INSERT OVERWRITE" in error

    # Rewriting the table with the current schema drops the old email values.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT OVERWRITE {spark_table(table_name)} SELECT * FROM {spark_table(table_name)}",
    )
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["email", "Nullable(String)"]]
    assert data == [["1", "\\N"], ["2", "\\N"], ["3", "carol@example.com"]]


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_across_rename_column(
    started_cluster_paimon_with_spark, storage_type
):
    """A caught-up incremental read continues across a RENAME COLUMN. A lagging
    one is refused with its cursor kept, and resumes once the cursor is moved to
    the last snapshot committed before the rename."""
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_inc_" + storage_type + "_" + get_uuid_str()

    instance = create_incremental_paimon_table(
        started_cluster, storage_type, table_name
    )
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        execute_spark_query(
            started_cluster,
            storage_type,
            table_name,
            f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN val TO val2",
        )
        execute_spark_query(
            started_cluster,
            storage_type,
            table_name,
            f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one')",
        )
        wait_until_query_result(instance, f"SELECT * FROM {table_name}", "1\tone\n")

        keeper_path = f"/clickhouse/paimon/{table_name}"
        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            cursor = zk.get(f"{keeper_path}/committed_snapshot")[0]

            # Not consumed before the next rename: its delta holds a file written with column val2.
            execute_spark_query(
                started_cluster,
                storage_type,
                table_name,
                f"INSERT INTO {spark_table(table_name)} VALUES (2, 'two')",
            )
            before_rename = spark.sql(
                f"SELECT max(snapshot_id) FROM {PAIMON_CATALOG}.default.`{table_name}$snapshots`"
            ).collect()[0][0]
            execute_spark_query(
                started_cluster,
                storage_type,
                table_name,
                f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN val2 TO val3",
            )
            execute_spark_query(
                started_cluster,
                storage_type,
                table_name,
                f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')",
            )

            def assert_refused(query):
                error = instance.query_and_get_error(query)
                assert "NOT_IMPLEMENTED" in error
                assert "field id 1 is column 'val2'" in error
                assert "but column 'val3'" in error
                return error

            for _ in range(2):
                error = assert_refused(f"SELECT * FROM {table_name}")
                assert f"set '{keeper_path}/committed_snapshot'" in error
                assert zk.get(f"{keeper_path}/committed_snapshot")[0] == cursor
            assert_refused(
                f"SELECT * FROM {table_name} SETTINGS paimon_target_snapshot_id = {before_rename}"
            )

            # Abandons only the snapshot of (2, 'two').
            zk.set(f"{keeper_path}/committed_snapshot", str(before_rename).encode())
            assert instance.query(f"SELECT * FROM {table_name}") == "3\tthree\n"
        finally:
            zk.stop()
