from helpers.paimon_utils import (
    assert_cluster_fan_out,
    create_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    execute_spark_query,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    parse_tsv,
    spark_table,
)


def create_one_row_table(started_cluster, prefix):
    name = f"{prefix}_{get_uuid_str()}"
    create_spark_paimon_table(
        started_cluster.spark_session, name, "id BIGINT NOT NULL, a STRING"
    )
    execute_spark_query(
        started_cluster, "s3", name, f"INSERT INTO {spark_table(name)} VALUES (1, 'x')"
    )
    return name


def alter_without_commit(started_cluster, name, alter_sql):
    execute_spark_query(
        started_cluster, "s3", name, f"ALTER TABLE {spark_table(name)} {alter_sql}"
    )
    spark = started_cluster.spark_session
    # The schema change writes a schema file but no snapshot.
    snapshots = (
        spark.sql(f"SELECT count(*) AS n FROM paimon.default.`{name}$snapshots`")
        .collect()[0]
        .n
    )
    schemas = (
        spark.sql(f"SELECT count(*) AS n FROM paimon.default.`{name}$schemas`")
        .collect()[0]
        .n
    )
    assert (snapshots, schemas) == (1, 2), (snapshots, schemas)


def test_add_column_without_commit(started_cluster_paimon_with_spark):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    name = create_one_row_table(started_cluster, "test_add_column_without_commit")
    table_expr = get_creation_expression(
        "s3", name, started_cluster, table_function=True
    )
    cluster_table_expr = get_creation_expression(
        "s3", name, started_cluster, table_function=True, run_on_cluster=True
    )

    with drop_tables_at_exit(instance, name):
        create_paimon_table("s3", instance, name, started_cluster)
        assert instance.query(f"SELECT * FROM {name}") == "1\tx\n"

        alter_without_commit(started_cluster, name, "ADD COLUMNS (b INT)")

        schema, data = get_schema_and_data(instance, table_expr)
        assert schema == [
            ["id", "Int64"],
            ["a", "Nullable(String)"],
            ["b", "Nullable(Int32)"],
        ]
        assert data == [["1", "x", "\\N"]]

        assert instance.query(f"SELECT id, a, b FROM {name}") == "1\tx\t\\N\n"

        query_id = f"paimon-cluster-{get_uuid_str()}"
        assert (
            instance.query(
                f"SELECT id, a, b FROM {cluster_table_expr}", query_id=query_id
            )
            == "1\tx\t\\N\n"
        )
        assert_cluster_fan_out(started_cluster.instances["node2"], query_id)


def test_rename_column_without_commit(started_cluster_paimon_with_spark):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    name = create_one_row_table(started_cluster, "test_rename_column_without_commit")
    table_expr = get_creation_expression(
        "s3", name, started_cluster, table_function=True
    )

    alter_without_commit(started_cluster, name, "RENAME COLUMN a TO c")

    # Values of `c` in files written before the rename depend on field-id mapping, not checked here.
    schema = [row[:2] for row in parse_tsv(instance.query(f"DESC {table_expr}"))]
    assert schema == [["id", "Int64"], ["c", "Nullable(String)"]]
    assert "UNKNOWN_IDENTIFIER" in instance.query_and_get_error(
        f"SELECT a FROM {table_expr}"
    )


def test_drop_column_without_commit(started_cluster_paimon_with_spark):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    name = create_one_row_table(started_cluster, "test_drop_column_without_commit")
    table_expr = get_creation_expression(
        "s3", name, started_cluster, table_function=True
    )

    alter_without_commit(started_cluster, name, "DROP COLUMN a")

    schema, data = get_schema_and_data(instance, table_expr)
    assert schema == [["id", "Int64"]]
    assert data == [["1"]]
