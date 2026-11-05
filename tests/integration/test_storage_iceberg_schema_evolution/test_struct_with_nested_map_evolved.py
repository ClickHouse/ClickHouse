import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    get_creation_expression,
    check_schema_and_data
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_struct_with_nested_map_evolved(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_struct_with_nested_map_evolved_"
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
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                outer STRUCT<
                    entries: MAP<INT, STRUCT<
                        a: INT,
                        b: STRING
                    >>
                >
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
                (named_struct('entries', MAP(1, named_struct('a', 10, 'b', 'hello'))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['outer', 'Tuple(\\n    entries Map(Int32, Tuple(\\n        a Nullable(Int32),\\n        b Nullable(String))))'],
        ],
        [
            ["({1:(10,'hello')})"],
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN outer.entries.value.c INT")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['outer', 'Tuple(\\n    entries Map(Int32, Tuple(\\n        a Nullable(Int32),\\n        b Nullable(String),\\n        c Nullable(Int32))))'],
        ],
        [
            ["({1:(10,'hello',NULL)})"],
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN outer.entries.value.c FIRST")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['outer', 'Tuple(\\n    entries Map(Int32, Tuple(\\n        c Nullable(Int32),\\n        a Nullable(Int32),\\n        b Nullable(String))))'],
        ],
        [
            ["({1:(NULL,10,'hello')})"],
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN outer.entries.value.b")

    check_schema_and_data(
        instance,
        table_function,
        [
            ['outer', 'Tuple(\\n    entries Map(Int32, Tuple(\\n        c Nullable(Int32),\\n        a Nullable(Int32))))'],
        ],
        [
            ["({1:(NULL,10)})"],
        ],
    )
