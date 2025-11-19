import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_minmax_pruning(started_cluster_iceberg_with_spark, storage_type, is_table_function):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_minmax_pruning_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                tag INT,
                date DATE,
                ts TIMESTAMP,
                time_struct struct<a : DATE, b : TIMESTAMP>,
                name VARCHAR(50),
                number BIGINT
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, DATE '2024-01-20',
        TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-01-20', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'vasya', 5)
    """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, DATE '2024-02-20',
        TIMESTAMP '2024-03-20 15:00:00', named_struct('a', DATE '2024-02-20', 'b', TIMESTAMP '2024-03-20 14:00:00'), 'vasilisa', 6)
    """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, DATE '2025-03-20',
        TIMESTAMP '2024-04-30 14:00:00', named_struct('a', DATE '2024-03-20', 'b', TIMESTAMP '2024-04-30 14:00:00'), 'icebreaker', 7)
    """
    )
    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, DATE '2025-04-20',
        TIMESTAMP '2024-05-30 14:00:00', named_struct('a', DATE '2024-04-20', 'b', TIMESTAMP '2024-05-30 14:00:00'), 'iceberg', 8)
    """
    )

    if is_table_function:
        creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )
    else:
        instance.query(get_creation_expression(
            storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=False
        ))
        creation_expression = TABLE_NAME

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergMinMaxIndexPrunedFiles', select_expression
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} ORDER BY ALL"
        )
        == 0
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE date <= '2024-01-25' ORDER BY ALL"
        )
        == 3
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE tag == 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE tag <= 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE name == 'vasilisa' ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE name < 'kek' ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE number == 8 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE number <= 5 ORDER BY ALL"
        )
        == 3
    )

    if not is_table_function:
        return

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN date TO date3")

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE date3 <= '2024-01-25' ORDER BY ALL"
        )
        == 3
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN tag TYPE BIGINT")

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE tag <= 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL"
        )
        == 3
    )

    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, DATE '2024-01-20', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-03-15', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'kek', 10)"
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL"
        )
        == 4
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD COLUMNS (ddd decimal(10, 3))")

    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, DATE '2024-01-20', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-03-15', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'kek', 30, decimal(17.22))"
    )

    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, DATE '2024-01-20', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-03-15', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'kek', 10, decimal(14311.772))"
    )

    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, DATE '2024-01-20', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-03-15', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'kek', 10, decimal(-8888.999))"
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE ddd >= 100 ORDER BY ALL"
        )
        == 2
    )
    # Spark store rounded values of decimals, this query checks that we work it around.
    # Please check the code where we parse lower bounds and upper bounds
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE ddd >= toDecimal64('17.21', 3) ORDER BY ALL"
        )
        == 1
    )