import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
    check_schema_and_data
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_tuple_evolved_nested(
    started_cluster_iceberg_schema_evolution, format_version, storage_type, is_table_function
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_tuple_evolved_nested_"
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
            a int NOT NULL,
            b struct<a: float, b: struct<na: float, nb: string>>,
            c struct<c : int, d: int>
        )
        USING iceberg 
        OPTIONS ('format-version'='2')
    """)

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (1, named_struct('a', 1.23, 'b', named_struct('na', 4.56, 'nb', 'BACCARA')), named_struct('c', 1, 'd', 2))")

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


    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        na Nullable(Float32),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.56,'BACCARA'))", '(1,2)']
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN b.b.na TO e")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float32),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.56,'BACCARA'))", '(1,2)']
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN b.b.e TYPE double;")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541,'BACCARA'))", '(1,2)']
        ],
    )
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN b.b.nb")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541))", '(1,2)']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN b.b.nc int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64),\\n        nc Nullable(Int32)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541,NULL))", '(1,2)']
        ],
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b.b.nc FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        nc Nullable(Int32),\\n        e Nullable(Float64)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(NULL,4.559999942779541))", '(1,2)']
        ],
    )
