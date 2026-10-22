import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
    check_schema_and_data
)


def make_query_executor(started_cluster, storage_type, table_name):
    spark = started_cluster.spark_session

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{table_name}/",
            f"/iceberg_data/default/{table_name}/",
        )

    return execute_spark_query


@pytest.mark.parametrize("storage_type", ["local"])
def test_map_of_arrays_evolved(started_cluster_iceberg_schema_evolution, storage_type):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    TABLE_NAME = "test_map_of_arrays_evolved_" + storage_type + "_" + get_uuid_str()
    execute_spark_query = make_query_executor(
        started_cluster_iceberg_schema_evolution, storage_type, TABLE_NAME
    )

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                entries MAP<INT, ARRAY<STRUCT<a: INT, b: STRING>>>
            )
            USING iceberg
            OPTIONS ('format-version'='2')
        """
    )
    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (MAP(1, ARRAY(named_struct('a', 10, 'b', 'hello'))))"
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['entries', 'Map(Int32, Array(Tuple(\\n    a Nullable(Int32),\\n    b Nullable(String))))'],
        ],
        [
            ["{1:[(10,'hello')]}"],
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN entries.value.element.c INT")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN entries.value.element.c FIRST")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['entries', 'Map(Int32, Array(Tuple(\\n    c Nullable(Int32),\\n    a Nullable(Int32),\\n    b Nullable(String))))'],
        ],
        [
            ["{1:[(NULL,10,'hello')]}"],
        ],
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_struct_with_doubly_nested_array_evolved(
    started_cluster_iceberg_schema_evolution, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    TABLE_NAME = (
        "test_struct_with_doubly_nested_array_evolved_" + storage_type + "_" + get_uuid_str()
    )
    execute_spark_query = make_query_executor(
        started_cluster_iceberg_schema_evolution, storage_type, TABLE_NAME
    )

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                top STRUCT<
                    lvl1: ARRAY<STRUCT<
                        lvl2: ARRAY<STRUCT<
                            a: INT,
                            b: STRING
                        >>
                    >>
                >
            )
            USING iceberg
            OPTIONS ('format-version'='2')
        """
    )
    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
                (named_struct('lvl1', ARRAY(named_struct('lvl2', ARRAY(named_struct('a', 1, 'b', 'x'))))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['top', 'Tuple(\\n    lvl1 Array(Tuple(\\n        lvl2 Array(Tuple(\\n            a Nullable(Int32),\\n            b Nullable(String))))))'],
        ],
        [
            ["([([(1,'x')])])"],
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN top.lvl1.element.lvl2.element.c INT")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN top.lvl1.element.lvl2.element.c FIRST")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN top.lvl1.element.lvl2.element.b")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['top', 'Tuple(\\n    lvl1 Array(Tuple(\\n        lvl2 Array(Tuple(\\n            c Nullable(Int32),\\n            a Nullable(Int32))))))'],
        ],
        [
            ["([([(NULL,1)])])"],
        ],
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_struct_with_sibling_arrays_evolved(
    started_cluster_iceberg_schema_evolution, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    TABLE_NAME = (
        "test_struct_with_sibling_arrays_evolved_" + storage_type + "_" + get_uuid_str()
    )
    execute_spark_query = make_query_executor(
        started_cluster_iceberg_schema_evolution, storage_type, TABLE_NAME
    )

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                top STRUCT<
                    x: ARRAY<STRUCT<a: INT>>,
                    y: ARRAY<STRUCT<b: INT>>
                >
            )
            USING iceberg
            OPTIONS ('format-version'='2')
        """
    )
    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
                (named_struct('x', ARRAY(named_struct('a', 1)), 'y', ARRAY(named_struct('b', 2))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN top.x.element.c INT")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN top.y.element.d INT")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN top.x.element.c FIRST")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['top', 'Tuple(\\n    x Array(Tuple(\\n        c Nullable(Int32),\\n        a Nullable(Int32))),\\n    y Array(Tuple(\\n        b Nullable(Int32),\\n        d Nullable(Int32))))'],
        ],
        [
            ["([(NULL,1)],[(2,NULL)])"],
        ],
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_struct_with_primitive_array_sibling_evolved(
    started_cluster_iceberg_schema_evolution, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    TABLE_NAME = (
        "test_struct_with_primitive_array_sibling_evolved_"
        + storage_type
        + "_"
        + get_uuid_str()
    )
    execute_spark_query = make_query_executor(
        started_cluster_iceberg_schema_evolution, storage_type, TABLE_NAME
    )

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                top STRUCT<
                    nums: ARRAY<INT>,
                    items: ARRAY<STRUCT<a: INT, b: STRING>>
                >
            )
            USING iceberg
            OPTIONS ('format-version'='2')
        """
    )
    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
                (named_struct('nums', ARRAY(1, 2), 'items', ARRAY(named_struct('a', 1, 'b', 'x')))),
                (named_struct('nums', CAST(ARRAY() AS ARRAY<INT>), 'items', CAST(ARRAY() AS ARRAY<STRUCT<a: INT, b: STRING>>))),
                (named_struct('nums', ARRAY(3), 'items', ARRAY(named_struct('a', 2, 'b', 'y'), named_struct('a', 3, 'b', 'z'))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN top.items.element.c INT")
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN top.items.element.c FIRST")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['top', 'Tuple(\\n    nums Array(Nullable(Int32)),\\n    items Array(Tuple(\\n        c Nullable(Int32),\\n        a Nullable(Int32),\\n        b Nullable(String))))'],
        ],
        [
            ["([],[])"],
            ["([1,2],[(NULL,1,'x')])"],
            ["([3],[(NULL,2,'y'),(NULL,3,'z')])"],
        ],
    )
