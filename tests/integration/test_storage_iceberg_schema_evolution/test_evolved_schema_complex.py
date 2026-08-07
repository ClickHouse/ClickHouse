import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    get_creation_expression,
    execute_spark_query_general,
    check_schema_and_data
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_evolved_schema_complex(started_cluster_iceberg_schema_evolution, format_version, storage_type):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_evolved_schema_complex_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_schema_evolution,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address STRUCT<
                    house_number : DOUBLE,
                    city: STRUCT<
                        name: STRING,
                        zip: INT
                    >
                >,
                animals ARRAY<INT>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (named_struct('house_number', 3, 'city', named_struct('name', 'Singapore', 'zip', 12345)), ARRAY(4, 7));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.appartment INT );
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 
             'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)),\\n    appartment Nullable(Int32))'],
            ['animals',
                'Array(Nullable(Int32))'],
        ],
        [
            ["(3,('Singapore',12345),NULL)", '[4,7]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.appartment;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 
             'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ["animals", "Array(Nullable(Int32))"],
        ],
        [["(3,('Singapore',12345))", "[4,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN animals.element TYPE BIGINT
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]']
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( map_column Map<INT, INT> );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (named_struct('house_number', 4, 'city', named_struct('name', 'Moscow', 'zip', 54321)), ARRAY(4, 7), MAP(1, 2));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
            ['map_column', 'Map(Int32, Nullable(Int32))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]', '{}'],
           ["(4,('Moscow',54321))", '[4,7]', '{1:2}'],
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN map_column TO col_to_del;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
            ['col_to_del', 'Map(Int32, Nullable(Int32))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]', '{}'],
           ["(4,('Moscow',54321))", '[4,7]', '{1:2}'],
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN col_to_del;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]'],
           ["(4,('Moscow',54321))", '[4,7]'],
        ]
    )

