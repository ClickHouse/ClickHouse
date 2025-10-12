import pytest
import uuid

from helpers.iceberg_utils import (
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
@pytest.mark.parametrize("run_on_cluster", [False, True])
def test_read_constant_columns_optimization(started_cluster_iceberg_with_spark, storage_type, run_on_cluster):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_read_constant_columns_optimization_" + storage_type + "_" + get_uuid_str()

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
                name VARCHAR(50),
                number BIGINT
            )
            USING iceberg
            PARTITIONED BY (identity(tag), years(date))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, DATE '2024-01-20', DATE '2024-01-20', 'vasya', 5),
        (1, DATE '2024-01-20', DATE '2024-01-20', 'vasilisa', 5),
        (1, DATE '2025-01-20', DATE '2025-01-20', 'vasya', 5),
        (1, DATE '2025-01-20', DATE '2025-01-20', 'vasya', 5),
        (2, DATE '2025-01-20', DATE '2025-01-20', 'vasilisa', 5),
        (2, DATE '2025-01-21', DATE '2025-01-20', 'vasilisa', 5)
    """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN number FIRST;
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (5, 3, DATE '2025-01-20', DATE '2024-01-20', 'vasilisa'),
        (5, 3, DATE '2025-01-20', DATE '2025-01-20', 'vasilisa')
    """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN name TO name_old;
        """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMNS (
                name string
            );
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (5, 4, DATE '2025-01-20', DATE '2024-01-20', 'vasya', 'iceberg'),
        (5, 4, DATE '2025-01-20', DATE '2025-01-20', 'vasilisa', 'iceberg'),
        (5, 5, DATE '2025-01-20', DATE '2024-01-20', 'vasya', 'iceberg'),
        (5, 5, DATE '2025-01-20', DATE '2024-01-20', 'vasilisa', 'icebreaker'),
        (5, 6, DATE '2025-01-20', DATE '2024-01-20', 'vasya', 'iceberg'),
        (5, 6, DATE '2025-01-20', DATE '2024-01-20', 'vasya', 'iceberg')
    """
    )

    # Totally must be 7 files
    # Partitioned column 'tag' is constant in each file
    # Column 'date' is constant in 6 files, has different values in (2-2025)
    # Column 'date2' is constant in 4 files (1-2024, 2-2025, 5-2025, 6-2025)
    # Column 'name_old' is constant in 3 files (1-2025, 2-2025 as 'name', 6-2025 as 'name_old')
    # Column 'number' is globally constant
    # New column 'name2' is present only in 3 files (4-2025, 5-2025, 6-2025), constant in two (4-2025, 6-2025)
    # Files 1-2025 and 6-2025 have only constant columns

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True, run_on_cluster=run_on_cluster
    )

    settings = "SETTINGS allow_experimental_iceberg_read_optimization=0"

    # Warm up metadata cache
    for replica in started_cluster_iceberg_with_spark.instances.values():
        replica.query("SYSTEM DROP ICEBERG METADATA CACHE")
        replica.query(f"SELECT * FROM {creation_expression} ORDER BY ALL {settings}")

    all_data_expected_query_id = str(uuid.uuid4())
    all_data_expected = instance.query(
        f"SELECT * FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=all_data_expected_query_id,
        )
    const_only_expected_query_id = str(uuid.uuid4())
    const_only_expected = instance.query(
        f"SELECT tag, number FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_only_expected_query_id,
        )
    const_partial_expected_query_id = str(uuid.uuid4())
    const_partial_expected = instance.query(
        f"SELECT tag, date2, number, name_old FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_partial_expected_query_id,
        )
    const_partial2_expected_query_id = str(uuid.uuid4())
    const_partial2_expected = instance.query(
        f"SELECT tag, date2, number, name FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_partial2_expected_query_id,
        )
    count_expected_query_id = str(uuid.uuid4())
    count_expected = instance.query(
        f"SELECT count(),tag FROM {creation_expression} GROUP BY ALL ORDER BY ALL {settings}",
        query_id=count_expected_query_id,
        )

    settings = "SETTINGS allow_experimental_iceberg_read_optimization=1"

    all_data_query_id = str(uuid.uuid4())
    all_data_optimized = instance.query(
        f"SELECT * FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=all_data_query_id,
        )
    const_only_query_id = str(uuid.uuid4())
    const_only_optimized = instance.query(
        f"SELECT tag, number FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_only_query_id,
        )
    const_partial_query_id = str(uuid.uuid4())
    const_partial_optimized = instance.query(
        f"SELECT tag, date2, number, name_old FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_partial_query_id,
        )
    const_partial2_query_id = str(uuid.uuid4())
    const_partial2_optimized = instance.query(
        f"SELECT tag, date2, number, name FROM {creation_expression} ORDER BY ALL {settings}",
        query_id=const_partial2_query_id,
        )
    count_query_id = str(uuid.uuid4())
    count_optimized = instance.query(
        f"SELECT count(),tag FROM {creation_expression} GROUP BY ALL ORDER BY ALL {settings}",
        query_id=count_query_id,
        )

    assert all_data_expected == all_data_optimized
    assert const_only_expected == const_only_optimized
    assert const_partial_expected == const_partial_optimized
    assert const_partial2_expected == const_partial2_optimized
    assert count_expected == count_optimized

    for replica in started_cluster_iceberg_with_spark.instances.values():
        replica.query("SYSTEM FLUSH LOGS")

    def check_events(query_id, event, expected):
        res = instance.query(
            f"""
            SELECT
                sum(tupleElement(arrayJoin(ProfileEvents),2)) as value
            FROM
                clusterAllReplicas('cluster_simple', system.query_log)
            WHERE
                type='QueryFinish'
                AND tupleElement(arrayJoin(ProfileEvents),1)='{event}'
                AND initial_query_id='{query_id}'
            GROUP BY ALL
            FORMAT CSV
            """)
        assert int(res) == expected

    event = "S3GetObject" if storage_type == "s3" else "AzureGetObject"

    # Without optimization clickhouse reads all 7 files
    check_events(all_data_expected_query_id, event, 7)
    check_events(const_only_expected_query_id, event, 7)
    check_events(const_partial_expected_query_id, event, 7)
    check_events(const_partial2_expected_query_id, event, 7)
    check_events(count_expected_query_id, event, 7)

    # If file has only constant columns it is not read
    check_events(all_data_query_id, event, 5) # 1-2025, 6-2025 must not be read
    check_events(const_only_query_id, event, 0) # All must not be read
    check_events(const_partial_query_id, event, 4) # 1-2025, 6-2025 and 2-2025 must not be read
    check_events(const_partial2_query_id, event, 3) # 6-2025 must not be read, 1-2024, 1-2025, 2-2025 don't have new column 'name'
    check_events(count_query_id, event, 0) # All must not be read

    def compare_selects(query):
        result_expected = instance.query(f"{query} SETTINGS allow_experimental_iceberg_read_optimization=0")
        result_optimized = instance.query(f"{query} SETTINGS allow_experimental_iceberg_read_optimization=1")
        assert result_expected == result_optimized

    compare_selects(f"SELECT _path,* FROM {creation_expression} ORDER BY ALL")
    compare_selects(f"SELECT _path,* FROM {creation_expression} WHERE name_old='vasily' ORDER BY ALL")
    compare_selects(f"SELECT _path,* FROM {creation_expression} WHERE ((tag + length(name_old)) % 2 = 1) ORDER BY ALL")