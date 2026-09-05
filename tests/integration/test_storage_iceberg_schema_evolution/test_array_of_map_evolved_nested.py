
import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    check_schema_and_data,
    default_upload_directory,
    get_creation_expression
)


# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106207
#
# When an Iceberg column is an `ARRAY` whose element is a `MAP` holding a
# `STRUCT` value, evolving that inner struct forces the schema transform to
# descend `ARRAY` -> (map element) -> `STRUCT`. The `ARRAY` branch of
# `IIcebergSchemaTransform::transform` used to move the wrong (empty) variable
# into `current_node` for a map element, corrupting the evolved schema.
@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_of_map_evolved_nested(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_array_of_map_evolved_nested_"
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

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address ARRAY<MAP<STRING, STRUCT<
                    foo: STRING,
                    bar: INT
                >>>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(MAP('key1', named_struct('foo', 'some_value', 'bar', 40))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )
    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Map(String, Tuple(\\n    foo Nullable(String),\\n    bar Nullable(Int32))))']
        ],
        [
            ["[{'key1':('some_value',40)}]"]
        ],
    )

    # Adding a field to the struct nested inside the map value of the array.
    # Reading the old data file now requires the ARRAY -> map-element -> STRUCT
    # descent that used to hit the buggy branch.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.value.baz INT );
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Map(String, Tuple(\\n    foo Nullable(String),\\n    bar Nullable(Int32),\\n    baz Nullable(Int32))))']
        ],
        [
            ["[{'key1':('some_value',40,NULL)}]"]
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(MAP('key2', named_struct('foo', 'some_value2', 'bar', 1, 'baz', 7))));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Map(String, Tuple(\\n    foo Nullable(String),\\n    bar Nullable(Int32),\\n    baz Nullable(Int32))))']
        ],
        [
            ["[{'key1':('some_value',40,NULL)}]"],
            ["[{'key2':('some_value2',1,7)}]"],
        ],
    )

    # Reorder the struct fields: the reordering transform must also descend
    # through the ARRAY -> map-element edge.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.value.baz FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Map(String, Tuple(\\n    baz Nullable(Int32),\\n    foo Nullable(String),\\n    bar Nullable(Int32))))']
        ],
        [
            ["[{'key1':(NULL,'some_value',40)}]"],
            ["[{'key2':(7,'some_value2',1)}]"],
        ],
    )

    # Drop a struct field: exercises the deleting transform on the same edge.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.value.foo;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Map(String, Tuple(\\n    baz Nullable(Int32),\\n    bar Nullable(Int32))))']
        ],
        [
            ["[{'key1':(NULL,40)}]"],
            ["[{'key2':(7,1)}]"],
        ],
    )
