import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    check_schema_and_data,
    default_upload_directory,
    get_creation_expression
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_map_evolved_with_struct(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_array_map_evolved_with_struct_"
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

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME};")

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                x ARRAY<MAP<INT, STRUCT<
                    a: INT,
                    b: STRING
                >>>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
                (ARRAY(MAP(1, named_struct('a', 10, 'b', 'hello')), MAP(2, named_struct('a', 20, 'b', 'world'))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    # Before evolution: the [ARRAY, MAP, STRUCT] traversal must read cleanly.
    check_schema_and_data(
        instance,
        table_function,
        [
            ['x', 'Array(Map(Int32, Tuple(\\n    a Nullable(Int32),\\n    b Nullable(String))))'],
        ],
        [
            ["[{1:(10,'hello')},{2:(20,'world')}]"],
        ],
    )

    # ADD a column to the innermost struct nested under Array(Map(...)).
    # This is what threw std::bad_variant_access before the fix.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN x.element.value.c INT;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['x', 'Array(Map(Int32, Tuple(\\n    a Nullable(Int32),\\n    b Nullable(String),\\n    c Nullable(Int32))))'],
        ],
        [
            ["[{1:(10,'hello',NULL)},{2:(20,'world',NULL)}]"],
        ],
    )

    # REORDER a struct field.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN x.element.value.c FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['x', 'Array(Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    a Nullable(Int32),\\n    b Nullable(String))))'],
        ],
        [
            ["[{1:(NULL,10,'hello')},{2:(NULL,20,'world')}]"],
        ],
    )

    # RENAME a struct field.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN x.element.value.a TO renamed_a;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['x', 'Array(Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    renamed_a Nullable(Int32),\\n    b Nullable(String))))'],
        ],
        [
            ["[{1:(NULL,10,'hello')},{2:(NULL,20,'world')}]"],
        ],
    )

    # DROP a struct field.
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN x.element.value.b;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['x', 'Array(Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    renamed_a Nullable(Int32))))'],
        ],
        [
            ["[{1:(NULL,10)},{2:(NULL,20)}]"],
        ],
    )
    return
