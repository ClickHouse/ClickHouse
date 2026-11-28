import pytest

from helpers.iceberg_utils import (
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


# Regression for issue #83462: reading an Iceberg table partitioned by the same
# source column more than once (here meta_ts appears in both hours(meta_ts) and
# an identity partition field) used to throw
#   Code 44 (ILLEGAL_COLUMN): Cannot add column `<id>`: column with this name already exists
# while building the partition-pruning key from the manifest partition spec.
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_multiple_partitions_on_one_column(
    started_cluster_iceberg_with_spark, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_multiple_partitions_on_one_column_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

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
                col1 STRING,
                col2 STRING,
                meta_ts TIMESTAMP,
                col3 STRING,
                col4 STRING,
                col5 STRING
            )
            USING iceberg
            PARTITIONED BY (col1, hours(meta_ts), col2, meta_ts)
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        ('val1', 'val2', timestamp('2024-01-01 00:00:00.000000'), 'val3', 'val4', 'val5')
    """
    )

    creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    # SELECT * forces file iteration, which builds the partition-pruning key.
    # count() alone is answered from manifest statistics and does not hit the bug.
    assert (
        instance.query(
            f"SELECT col1, col2, col3, col4, col5 FROM {creation_expression} ORDER BY ALL"
        ).strip()
        == "val1\tval2\tval3\tval4\tval5"
    )
    assert int(instance.query(f"SELECT count() FROM {creation_expression}")) == 1
