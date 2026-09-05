import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    check_schema_and_data,
    default_upload_directory,
    get_creation_expression
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_struct_with_array_and_map_evolved(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_struct_with_array_and_map_evolved_"
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
                x STRUCT<
                    residents: ARRAY<STRUCT<
                        name: STRING,
                        age: INT
                    >>,
                    neighbours: MAP<INT, STRUCT<
                        name: STRING,
                        age: INT
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
                (named_struct(
                    'residents', ARRAY(named_struct('name', 'Alice', 'age', 30)),
                    'neighbours', MAP(1, named_struct('name', 'Bob', 'age', 40))));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )

    initial_schema = (
        'Tuple(\\n'
        '    residents Array(Tuple(\\n'
        '        name Nullable(String),\\n'
        '        age Nullable(Int32))),\\n'
        '    neighbours Map(Int32, Tuple(\\n'
        '        name Nullable(String),\\n'
        '        age Nullable(Int32))))'
    )

    check_schema_and_data(
        instance,
        table_function,
        [['x', initial_schema]],
        [["([('Alice',30)],{1:('Bob',40)})"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN x.residents.element.phone STRING;
        """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN x.neighbours.value.phone STRING;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            [
                'x',
                'Tuple(\\n'
                '    residents Array(Tuple(\\n'
                '        name Nullable(String),\\n'
                '        age Nullable(Int32),\\n'
                '        phone Nullable(String))),\\n'
                '    neighbours Map(Int32, Tuple(\\n'
                '        name Nullable(String),\\n'
                '        age Nullable(Int32),\\n'
                '        phone Nullable(String))))',
            ]
        ],
        [["([('Alice',30,NULL)],{1:('Bob',40,NULL)})"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN x.residents.element.phone FIRST;
        """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN x.neighbours.value.name TO renamed_name;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            [
                'x',
                'Tuple(\\n'
                '    residents Array(Tuple(\\n'
                '        phone Nullable(String),\\n'
                '        name Nullable(String),\\n'
                '        age Nullable(Int32))),\\n'
                '    neighbours Map(Int32, Tuple(\\n'
                '        renamed_name Nullable(String),\\n'
                '        age Nullable(Int32),\\n'
                '        phone Nullable(String))))',
            ]
        ],
        [["([(NULL,'Alice',30)],{1:('Bob',40,NULL)})"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN x.residents.element.age;
        """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN x.neighbours.value.age;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            [
                'x',
                'Tuple(\\n'
                '    residents Array(Tuple(\\n'
                '        phone Nullable(String),\\n'
                '        name Nullable(String))),\\n'
                '    neighbours Map(Int32, Tuple(\\n'
                '        renamed_name Nullable(String),\\n'
                '        phone Nullable(String))))',
            ]
        ],
        [["([(NULL,'Alice')],{1:('Bob',NULL)})"]],
    )
    return
