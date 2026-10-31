import pytest

from helpers.paimon_utils import (
    TYPES_COLUMNS_SQL,
    TYPES_EXPECTED_DATA,
    TYPES_EXPECTED_SCHEMA,
    TYPES_NULLS_ROW,
    TYPES_VALUES_ROW,
    create_spark_paimon_table,
    get_creation_expression,
    get_schema_and_data,
    get_uuid_str,
    parse_tsv,
    spark_table,
    upload_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_all_types(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_all_types_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, TYPES_COLUMNS_SQL)
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES {TYPES_VALUES_ROW}")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES {TYPES_NULLS_ROW}")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    schema, data = get_schema_and_data(
        instance,
        table_expr,
        order_by="f_int_nn",
        settings={"session_timezone": "UTC"},
    )
    assert schema == TYPES_EXPECTED_SCHEMA
    assert data == TYPES_EXPECTED_DATA


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_row_type(started_cluster_paimon_with_spark, storage_type):
    """A Paimon ROW is read as a named Tuple, like an Iceberg or Delta Lake struct."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_row_type_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark,
        table_name,
        "id INT NOT NULL, s STRUCT<x: INT, y: STRING>, s_nn STRUCT<x: INT NOT NULL> NOT NULL, "
        "nested STRUCT<sub: STRUCT<z: BIGINT>, arr: ARRAY<INT>, m: MAP<STRING, INT>>, "
        "arr_s ARRAY<STRUCT<x: INT>>, map_s MAP<STRING, STRUCT<x: INT>>",
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} VALUES "
        "(1, named_struct('x', 1, 'y', 'a'), named_struct('x', 2), "
        "named_struct('sub', named_struct('z', 3L), 'arr', array(4, 5), 'm', map('k', 6)), "
        "array(named_struct('x', 7)), map('k', named_struct('x', 8))), "
        "(2, NULL, named_struct('x', 9), NULL, "
        "array(CAST(NULL AS STRUCT<x: INT>)), map('k', CAST(NULL AS STRUCT<x: INT>)))"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    schema = [
        row[:2]
        for row in parse_tsv(
            instance.query(
                f"DESC {table_expr}", settings={"print_pretty_type_names": 0}
            )
        )
    ]
    assert schema == [
        ["id", "Int32"],
        ["s", "Tuple(x Nullable(Int32), y Nullable(String))"],
        ["s_nn", "Tuple(x Int32)"],
        [
            "nested",
            "Tuple(sub Tuple(z Nullable(Int64)), arr Array(Nullable(Int32)), "
            "m Map(String, Nullable(Int32)))",
        ],
        ["arr_s", "Array(Tuple(x Nullable(Int32)))"],
        ["map_s", "Map(String, Tuple(x Nullable(Int32)))"],
    ]
    assert instance.query(f"SELECT * FROM {table_expr} ORDER BY id") == (
        "1\t(1,'a')\t(2)\t((3),[4,5],{'k':6})\t[(7)]\t{'k':(8)}\n"
        "2\t(NULL,NULL)\t(9)\t((NULL),[],{})\t[(NULL)]\t{'k':(NULL)}\n"
    )
    assert instance.query(f"SELECT id FROM {table_expr} ORDER BY id") == "1\n2\n"
    assert (
        instance.query(f"SELECT s.y, nested.sub.z FROM {table_expr} ORDER BY id")
        == "a\t3\n\\N\t\\N\n"
    )
