import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    check_schema_and_data,
    default_upload_directory,
    get_creation_expression
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_evolved_with_struct(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_array_evolved_with_struct_"
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
                address ARRAY<STRUCT<
                    city: STRING,
                    zip: INT
                >>,
                values ARRAY<INT>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('name', 'Singapore', 'zip', 12345), named_struct('name', 'Moscow', 'zip', 54321)), ARRAY(1,2));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.foo INT );
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Nullable(String),\\n    zip Nullable(Int32),\\n    foo Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[('Singapore',12345,NULL),('Moscow',54321,NULL)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.city;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zip Nullable(Int32),\\n    foo Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(12345,NULL),(54321,NULL)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.foo FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    foo Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN address.element.foo TO city;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN address TO bee;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN values TO fee;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['fee', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN fee.element TYPE long;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['fee', 'Array(Nullable(Int64))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )
    return
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN fee FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['fee', 'Array(Nullable(Int64))'],
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))']
        ],
        [
            ['[1,2]', "[(NULL,12345),(NULL,54321)]"]
        ],
    )
