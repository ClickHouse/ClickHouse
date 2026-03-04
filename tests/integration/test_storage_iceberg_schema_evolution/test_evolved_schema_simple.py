import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    get_creation_expression,
    execute_spark_query_general,
    check_schema_and_data
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_evolved_schema_simple(
    started_cluster_iceberg_schema_evolution, format_version, storage_type, is_table_function
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
    TABLE_NAME = (
        "test_evolved_schema_simple_"
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
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                a int NOT NULL,
                b float,
                c decimal(9,2) NOT NULL,
                d array<int>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

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
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (4, NULL, 7.12, ARRAY(5, 6, 7));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b TYPE double;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (7, 5.0, 18.1, ARRAY(6, 7, 9));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "4", "\\N", "7.12"], ["[6,7,9]", "7", "5", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "\\N", "4", "7.12"], ["[6,7,9]", "5", "7", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMNS (
                e string
            );
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c TYPE decimal(12, 2);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(5, 6, 7), 3, -30, 7.12, 'AAA');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a TYPE BIGINT;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(), 3.0, 12, -9.13, 'BBB');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a DROP NOT NULL;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (NULL, 3.4, NULL, -9.13, NULL);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[]", "3.4", "\\N", "-9.13", "\\N"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN a TO f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["f", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )
    if not is_table_function :
        print (instance.query("SELECT * FROM system.iceberg_history"))
        assert int(instance.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{TABLE_NAME}'")) == 5
        assert int(instance.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{TABLE_NAME}' AND made_current_at >= yesterday()")) == 5

    # Do a single check to verify that restarting CH maintains the setting (ATTACH)
    # We are just interested on the setting working after restart, so no need to run it on all combinations
    if format_version == "1" and storage_type == "s3" and not is_table_function:

        instance.restart_clickhouse()

        execute_spark_query(
            f"""
                ALTER TABLE {TABLE_NAME} RENAME COLUMN e TO z;
            """
        )

        check_schema_and_data(
            instance,
            table_select_expression,
            [
                ["b", "Nullable(Float64)"],
                ["f", "Nullable(Int64)"],
                ["c", "Decimal(12, 2)"],
                ["z", "Nullable(String)"],
            ],
            [
                ["3", "-30", "7.12", "AAA"],
                ["3", "12", "-9.13", "BBB"],
                ["3.4", "\\N", "-9.13", "\\N"],
                ["5", "7", "18.1", "\\N"],
                ["\\N", "4", "7.12", "\\N"],
            ],
        )