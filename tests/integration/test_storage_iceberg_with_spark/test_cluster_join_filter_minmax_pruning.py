import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3"])
def test_cluster_join_filter_minmax_pruning(started_cluster_iceberg_with_spark, storage_type):
    """
    icebergCluster lists files on the initiator. A left-only WHERE on
    count() of SELECT * … JOIN must still reach that listing so min/max
    pruning can skip files (the original icebergCluster JOIN subquery case).
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_cluster_join_filter_minmax_pruning_" + storage_type + "_" + get_uuid_str()
    BAR_NAME = "bar_" + storage_type + "_" + get_uuid_str()

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
                datetime DATE,
                symbol VARCHAR(50),
                bid INT
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (DATE '2024-01-01', 'AAPL', 1)")
    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (DATE '2024-01-02', 'AAPL', 2)")
    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (DATE '2024-01-03', 'AAPL', 3)")
    # Passes `bid >= 3`, fails `datetime >= 2024-01-03`. Distinguishes listing
    # that only saw the inner JOIN `WHERE` from listing that also got the outer `WHERE`.
    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (DATE '2024-01-01', 'AAPL', 4)")

    iceberg = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )

    instance.query(
        f"CREATE TABLE `{BAR_NAME}` (symbol String, comment String) ENGINE = Memory"
    )
    instance.query(
        f"INSERT INTO `{BAR_NAME}` VALUES ('AAPL', 'comment'), ('AAPL2', 'comment2')"
    )

    common_settings = {
        "input_format_parquet_bloom_filter_push_down": 0,
        "input_format_parquet_filter_push_down": 0,
        "query_plan_filter_push_down": 1,
        "enable_analyzer": 1,
        "query_plan_join_swap_table": 0,
        "enable_join_runtime_filters": 0,
        "enable_parallel_replicas": 0,
        "join_use_nulls": 1,
    }

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {**common_settings, "use_iceberg_partition_pruning": 0}
        settings2 = {**common_settings, "use_iceberg_partition_pruning": 1}
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            settings1,
            settings2,
            "IcebergMinMaxIndexPrunedFiles",
            select_expression,
        )

    # Four data files: bid 1/2/3/4. `bid >= 3` keeps two files (prunes 2).
    expected_pruned = 2

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT count() FROM {iceberg} WHERE bid >= 3"
        )
        == expected_pruned
    )

    assert (
        check_validity_and_get_prunned_files(
            f"""
            SELECT count()
            FROM {iceberg} AS foo
            LEFT JOIN `{BAR_NAME}` AS bar ON foo.symbol = bar.symbol
            WHERE foo.bid >= 3
            """
        )
        == expected_pruned
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT count() FROM (SELECT * FROM {iceberg} AS foo WHERE foo.bid >= 3)"
        )
        == expected_pruned
    )

    assert (
        check_validity_and_get_prunned_files(
            f"""
            SELECT count()
            FROM
            (
                SELECT *
                FROM {iceberg} AS foo
                LEFT JOIN `{BAR_NAME}` AS bar ON foo.symbol = bar.symbol
                WHERE foo.bid >= 3
            )
            """
        )
        == expected_pruned
    )

    # Inner `bid >= 3` is copied onto the cluster wrap during planning. The outer
    # `datetime` predicate is pushed later; listing must AND it onto the wrap
    # DAG or the extra file with bid=4 / datetime=2024-01-01 is not pruned.
    assert (
        check_validity_and_get_prunned_files(
            f"""
            SELECT count()
            FROM
            (
                SELECT *
                FROM {iceberg} AS foo
                LEFT JOIN `{BAR_NAME}` AS bar ON foo.symbol = bar.symbol
                WHERE foo.bid >= 3
            )
            WHERE datetime >= '2024-01-03'
            """
        )
        == 3
    )
