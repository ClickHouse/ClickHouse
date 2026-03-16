import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

@pytest.mark.parametrize(
    "storage_type, run_on_cluster",
    [("s3", False), ("s3", True), ("azure", False), ("local", False)],
)
def test_partition_pruning(started_cluster_iceberg_with_spark, storage_type, run_on_cluster):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_partition_pruning_" + storage_type + "_" + get_uuid_str()

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
                date2 DATE,
                ts TIMESTAMP,
                ts2 TIMESTAMP,
                time_struct struct<a : DATE, b : TIMESTAMP>,
                name VARCHAR(50),
                number BIGINT
            )
            USING iceberg
            PARTITIONED BY (identity(tag), days(date), years(date2), hours(ts), months(ts2), TRUNCATE(3, name), TRUNCATE(3, number))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, DATE '2024-01-20', DATE '2024-01-20',
        TIMESTAMP '2024-02-20 10:00:00', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-01-20', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'vasya', 5),
        (2, DATE '2024-01-30', DATE '2024-01-30',
        TIMESTAMP '2024-03-20 15:00:00', TIMESTAMP '2024-03-20 15:00:00', named_struct('a', DATE '2024-03-20', 'b', TIMESTAMP '2024-03-20 14:00:00'), 'vasilisa', 6),
        (1, DATE '2024-02-20', DATE '2024-02-20',
        TIMESTAMP '2024-03-20 20:00:00', TIMESTAMP '2024-03-20 20:00:00', named_struct('a', DATE '2024-02-20', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'iceberg', 7),
        (2, DATE '2025-01-20', DATE '2025-01-20',
        TIMESTAMP '2024-04-30 14:00:00', TIMESTAMP '2024-04-30 14:00:00', named_struct('a', DATE '2024-04-30', 'b', TIMESTAMP '2024-04-30 14:00:00'), 'icebreaker', 8);
    """
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True, run_on_cluster=run_on_cluster
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergPartitionPrunedFiles', select_expression
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
            f"SELECT * FROM {creation_expression} WHERE date2 <= '2024-01-25' ORDER BY ALL"
        )
        == 1
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 3
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE ts2 <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 1
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE tag == 1 ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE tag <= 1 ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE name == 'vasilisa' ORDER BY ALL"
        )
        == 2
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
        == 1
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE number <= 5 ORDER BY ALL"
        )
        == 3
    )

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
        == 2
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD time_struct.a")

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL"
        )
        == 0
    )

    execute_spark_query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, DATE '2024-01-20', DATE '2024-01-20', TIMESTAMP '2024-02-20 10:00:00', TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-03-15', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'kek', 10)"
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL"
        )
        == 1
    )

