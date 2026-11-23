import pytest

from helpers.paimon_utils import (
    PAIMON_CATALOG,
    assert_cluster_fan_out,
    create_incremental_paimon_table,
    create_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    execute_spark_query,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    query_profile_events,
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
    """Files written before a RENAME COLUMN store the field under its old name.
    They are read by field id, so the renamed column returns the old values."""
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

    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["b2", "Nullable(String)"]]
    assert data == [["1", "one"], ["2", "two"], ["3", "three"]]
    assert instance.query(f"SELECT a FROM {table_expr} WHERE b2 = 'one'") == "1\n"
    assert instance.query(f"SELECT count() FROM {table_expr}") == "3\n"

    if storage_type == "s3":
        # node2 reads every file, so the schema of the file written before the rename travels with the task.
        cluster_expr = get_creation_expression(
            storage_type,
            table_name,
            started_cluster,
            table_function=True,
            run_on_cluster=True,
            cluster_name="cluster_node2",
        )
        query_id = f"paimon-rename-cluster-{get_uuid_str()}"
        assert (
            instance.query(
                f"SELECT * FROM {cluster_expr} ORDER BY a", query_id=query_id
            )
            == "1\tone\n2\ttwo\n3\tthree\n"
        )
        assert_cluster_fan_out(started_cluster.instances["node2"], query_id)

    # A copy-on-write UPDATE rewrites the file written before the rename with the current schema.
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"UPDATE {spark_table(table_name)} SET b2 = 'uno' WHERE a = 1",
    )
    _, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert data == [["1", "uno"], ["2", "two"], ["3", "three"]]


@pytest.mark.parametrize("file_format", ["orc", "avro"])
def test_schema_evolution_rename_column_file_format(
    started_cluster_paimon_with_spark, file_format
):
    """The ORC and Avro readers also read a file written before a RENAME COLUMN by field id."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_rename_" + file_format + "_" + get_uuid_str()

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    table_expr = table_expr[:-1] + f", format = '{file_format.upper()}')"

    create_spark_paimon_table(
        spark,
        table_name,
        "a INT NOT NULL, b STRING",
        tblproperties={"file.format": file_format},
    )
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')",
    )
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN b TO b2",
    )
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')",
    )

    _, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert data == [["1", "one"], ["2", "two"], ["3", "three"]]


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_schema_evolution_drop_and_add_same_name(
    started_cluster_paimon_with_spark, storage_type
):
    """After DROP COLUMN email + ADD COLUMN email, files written before the drop
    hold the dropped field under the same name. The re-added column is another
    field id, so it reads NULL for them, also when the name differs only in case
    and the Parquet reader matches names case-insensitively."""
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

    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["email", "Nullable(String)"]]
    assert data == [["1", "\\N"], ["2", "\\N"], ["3", "carol@example.com"]]

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
        f"ALTER TABLE {spark_table(table_name)} ADD COLUMN EMAIL STRING",
    )
    execute_spark_query(
        started_cluster,
        storage_type,
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (4, 'dave@example.com')",
    )
    for settings in [
        None,
        {"input_format_parquet_case_insensitive_column_matching": 1},
    ]:
        schema, data = get_schema_and_data(
            instance, table_expr, order_by="a", settings=settings
        )
        assert schema == [["a", "Int32"], ["EMAIL", "Nullable(String)"]]
        assert data == [
            ["1", "\\N"],
            ["2", "\\N"],
            ["3", "\\N"],
            ["4", "dave@example.com"],
        ]


def test_schema_evolution_rotate_columns(started_cluster_paimon_with_spark):
    """After the names of three columns are rotated, every name of the old file
    belongs to another column of the same or another type. Neither the query
    condition cache, the reader's filter nor the transform may bind them by name.
    A table engine keeps its UUID, so its cache entries outlive the rename."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_rotate_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT, b INT, c STRING")
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (1, 2, 'x')",
    )

    with drop_tables_at_exit(instance, table_name):
        create_paimon_table("s3", instance, table_name, started_cluster)

        query = f"SELECT c, a, b FROM {table_name} WHERE a = 2 SETTINGS use_query_condition_cache = 1"
        assert instance.query(query) == ""
        # The query condition cache now has the entry that no row of the file matches a = 2.
        assert query_profile_events(
            instance, query, ["ObjectStorageQueryConditionCacheSkippedFiles"]
        ) == [1]

        for rename in ["a TO tmp", "b TO a", "c TO b", "tmp TO c"]:
            execute_spark_query(
                started_cluster,
                "s3",
                table_name,
                f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN {rename}",
            )
        execute_spark_query(
            started_cluster,
            "s3",
            table_name,
            f"INSERT INTO {spark_table(table_name)} VALUES (10, 20, 'y')",
        )

        assert (
            instance.query(
                f"SELECT c, a, b FROM {table_name} WHERE a = 2 SETTINGS use_query_condition_cache = 0"
            )
            == "1\t2\tx\n"
        )
        assert instance.query(query) == "1\t2\tx\n"
        assert instance.query(f"SELECT b FROM {table_name} ORDER BY b") == "x\ny\n"
        schema, data = get_schema_and_data(instance, table_name, order_by="c")
        assert schema == [
            ["c", "Nullable(Int32)"],
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
        ]
        assert data == [["1", "2", "x"], ["10", "20", "y"]]


def test_schema_evolution_no_shared_column(started_cluster_paimon_with_spark):
    """Every column of the old file was dropped and its names were added again,
    so the file has no field of the current schema: its rows read NULL."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_no_shared_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT, b STRING")
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (1, 'x')",
    )
    for alter in [
        "ADD COLUMN k INT",
        "DROP COLUMN a",
        "DROP COLUMN b",
        "ADD COLUMN a INT",
        "ADD COLUMN b STRING",
    ]:
        execute_spark_query(
            started_cluster,
            "s3",
            table_name,
            f"ALTER TABLE {spark_table(table_name)} {alter}",
        )
    execute_spark_query(
        started_cluster,
        "s3",
        table_name,
        f"INSERT INTO {spark_table(table_name)} VALUES (2, 3, 'y')",
    )

    for run_on_cluster in [False, True]:
        table_expr = get_creation_expression(
            "s3",
            table_name,
            started_cluster,
            table_function=True,
            run_on_cluster=run_on_cluster,
            cluster_name="cluster_node2",
        )
        assert (
            instance.query(f"SELECT k, a, b FROM {table_expr} ORDER BY k NULLS FIRST")
            == "\\N\t\\N\t\\N\n2\t3\ty\n"
        )


def test_schema_evolution_row_column(started_cluster_paimon_with_spark):
    """A tuple element of a ROW column is read from a file written before a
    RENAME COLUMN. Fields inside a ROW are matched by name, so a file written
    before one of them was renamed, or dropped and added again, is refused."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_schema_evolution_row_" + get_uuid_str()
    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )

    create_spark_paimon_table(
        spark, table_name, "id INT NOT NULL, s STRUCT<x: INT, y: STRING>"
    )
    for query in [
        f"INSERT INTO {spark_table(table_name)} VALUES (1, named_struct('x', 1, 'y', 'a'))",
        f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN s TO s2",
        f"INSERT INTO {spark_table(table_name)} VALUES (2, named_struct('x', 2, 'y', 'b'))",
    ]:
        execute_spark_query(started_cluster, "s3", table_name, query)

    assert (
        instance.query(f"SELECT id, s2.y FROM {table_expr} ORDER BY id")
        == "1\ta\n2\tb\n"
    )
    assert instance.query(f"SELECT id FROM {table_expr} WHERE s2.y = 'a'") == "1\n"
    assert (
        instance.query(f"SELECT id, s2 FROM {table_expr} ORDER BY id")
        == "1\t(1,'a')\n2\t(2,'b')\n"
    )

    for query in [
        f"ALTER TABLE {spark_table(table_name)} RENAME COLUMN s2.x TO x2",
        f"INSERT INTO {spark_table(table_name)} VALUES (3, named_struct('x2', 3, 'y', 'c'))",
    ]:
        execute_spark_query(started_cluster, "s3", table_name, query)
    error = instance.query_and_get_error(f"SELECT * FROM {table_expr}")
    assert "NOT_IMPLEMENTED" in error
    assert "field id 2 is 's" in error and "'s2.x2'" in error

    table_name = "test_schema_evolution_row_readd_" + get_uuid_str()
    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    create_spark_paimon_table(
        spark, table_name, "id INT NOT NULL, s STRUCT<x: INT, y: STRING>"
    )
    for query in [
        f"INSERT INTO {spark_table(table_name)} VALUES (1, named_struct('x', 1, 'y', 'a'))",
        f"ALTER TABLE {spark_table(table_name)} DROP COLUMN s.y",
        f"ALTER TABLE {spark_table(table_name)} ADD COLUMN s.y STRING",
        f"INSERT INTO {spark_table(table_name)} VALUES (2, named_struct('x', 2, 'y', 'b'))",
    ]:
        execute_spark_query(started_cluster, "s3", table_name, query)
    error = instance.query_and_get_error(f"SELECT * FROM {table_expr}")
    assert "NOT_IMPLEMENTED" in error
    assert "'s.y' is field id 3" in error


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_across_rename_column(
    started_cluster_paimon_with_spark, storage_type
):
    """An incremental read continues across a RENAME COLUMN, also when it
    reaches the snapshots committed before the rename only after it."""
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
        latest = spark.sql(
            f"SELECT max(snapshot_id) FROM {PAIMON_CATALOG}.default.`{table_name}$snapshots`"
        ).collect()[0][0]

        assert (
            instance.query(f"SELECT * FROM {table_name} ORDER BY id")
            == "2\ttwo\n3\tthree\n"
        )
        assert instance.query(f"SELECT * FROM {table_name}") == ""
        zk = started_cluster.get_kazoo_client("zoo1")
        try:
            committed = zk.get(f"/clickhouse/paimon/{table_name}/committed_snapshot")[0]
            assert int(committed) == latest
        finally:
            zk.stop()

        assert (
            instance.query(
                f"SELECT * FROM {table_name} SETTINGS paimon_target_snapshot_id = {before_rename}"
            )
            == "2\ttwo\n"
        )
