import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
    check_schema_and_data
)


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("is_table_function", [False])
def test_map_evolved_nested(
    started_cluster_iceberg_schema_evolution, format_version, storage_type, is_table_function
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_map_evolved_nested_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster_iceberg_schema_evolution,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            b Map<INT, INT>,
            a Map<INT, Struct<
                c : INT,
                d : String
            >>,
            c Struct <
                e : Map<Int, String>
            >
        )
        USING iceberg 
        OPTIONS ('format-version'='2')
    """)

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (MAP(1, 2), Map(3, named_struct('c', 4, 'd', 'ABBA')), named_struct('e', MAP(5, 'foo')))")

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_schema_evolution,
        table_function=is_table_function,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b.value TYPE long;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    e Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(4,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN c.e TO f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(4,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a.value.d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    d Nullable(String),\\n    c Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:('ABBA',4)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN a.value.g int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    d Nullable(String),\\n    c Nullable(Int32),\\n    g Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:('ABBA',4,NULL)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a.value.g FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    g Nullable(Int32),\\n    d Nullable(String),\\n    c Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA',4)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN a.value.c;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    g Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN a.value.g TO c;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["({5:'foo'})", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN c.g int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)),\\n    g Nullable(Int32))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["({5:'foo'},NULL)", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c.g FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    g Nullable(Int32),\\n    f Map(Int32, Nullable(String)))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["(NULL,{5:'foo'})", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN c.f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    g Nullable(Int32))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["(NULL)", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )
