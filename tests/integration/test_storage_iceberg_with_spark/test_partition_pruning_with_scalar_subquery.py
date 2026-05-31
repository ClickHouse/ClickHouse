import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105291
#
# A `WHERE partition_col = (SELECT max(...) FROM t)` filter must trigger Iceberg
# partition pruning. The analyzer wraps the scalar subquery result in an internal
# `_CAST(Const, 'TargetType')` function node. Before the fix in `KeyCondition.cpp`
# (`isTrivialCast`), only the user-facing name `CAST` was recognized, so the
# `_CAST` wrapper survived into `KeyCondition` as a FUNCTION node, and
# `RPNBuilder::tryGetConstant` could not extract the constant. The pruner then
# fell back to scanning all partitions.

@pytest.mark.parametrize(
    "storage_type",
    ["s3", "azure", "local"],
)
def test_partition_pruning_with_scalar_subquery(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_pp_scalar_sq_" + storage_type + "_" + get_uuid_str()

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
                id INT,
                data STRING
            )
            USING iceberg
            PARTITIONED BY (identity(id))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c'),
            (4, 'd'),
            (5, 'e')
        """
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergPartitionPrunedFiles', select_expression
        )

    # Scalar subquery on the RHS: the analyzer resolves it to a constant wrapped
    # in `_CAST(Const, 'Nullable(Int32)')`. With the fix in place 4 partitions
    # (1, 2, 3, 4) are pruned and only the partition for id = 5 remains.
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} "
            f"WHERE id = (SELECT max(id) FROM {creation_expression}) ORDER BY ALL"
        )
        == 4
    )
