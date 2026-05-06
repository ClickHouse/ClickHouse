import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    get_creation_expression,
    execute_spark_query_general,
)

# TODO - turn on after merge alternative syntax
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def _test_cluster_joins(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_cluster_joins_" + storage_type + "_" + get_uuid_str()
    TABLE_NAME_2 = "test_cluster_joins_2_" + storage_type + "_" + get_uuid_str()
    TABLE_NAME_LOCAL = "test_cluster_joins_local_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str, table_name):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            table_name,
            query,
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                tag INT,
                name VARCHAR(50)
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """, TABLE_NAME
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'john'),
        (2, 'jack')
    """, TABLE_NAME
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME_2} (
                id INT,
                second_name VARCHAR(50)
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """, TABLE_NAME_2
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME_2} VALUES
        (1, 'dow'),
        (2, 'sparrow')
    """, TABLE_NAME_2
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True, run_on_cluster=True
    )

    creation_expression_2 = get_creation_expression(
        storage_type, TABLE_NAME_2, started_cluster_iceberg_with_spark, table_function=True, run_on_cluster=True
    )

    instance.query(f"CREATE TABLE `{TABLE_NAME_LOCAL}` (id Int64, second_name String) ENGINE = Memory()")
    instance.query(f"INSERT INTO `{TABLE_NAME_LOCAL}` VALUES (1, 'silver'), (2, 'black')")

    res = instance.query(
        f"""
            SELECT t1.name,t2.second_name
            FROM {creation_expression} AS t1
                JOIN {creation_expression_2} AS t2
                ON t1.tag=t2.id
            ORDER BY ALL
            SETTINGS
                object_storage_cluster='cluster_simple',
                object_storage_cluster_join_mode='local'
        """
    )

    assert res == "jack\tsparrow\njohn\tdow\n"

    res = instance.query(
        f"""
            SELECT name
            FROM {creation_expression}
            WHERE tag in (
                SELECT id
                FROM {creation_expression_2}
            )
            ORDER BY ALL
            SETTINGS
                object_storage_cluster='cluster_simple',
                object_storage_cluster_join_mode='local'
        """
    )

    assert res == "jack\njohn\n"

    res = instance.query(
        f"""
            SELECT t1.name,t2.second_name
            FROM {creation_expression} AS t1
                JOIN `{TABLE_NAME_LOCAL}` AS t2
                ON t1.tag=t2.id
            WHERE t1.tag < 10 AND t2.id < 20
            ORDER BY ALL
            SETTINGS
                object_storage_cluster='cluster_simple',
                object_storage_cluster_join_mode='local'
        """
    )

    assert res == "jack\tblack\njohn\tsilver\n"

    res = instance.query(
        f"""
            SELECT name
            FROM {creation_expression}
            WHERE tag in (
                SELECT id
                FROM `{TABLE_NAME_LOCAL}`
            )
            ORDER BY ALL
            SETTINGS
                object_storage_cluster='cluster_simple',
                object_storage_cluster_join_mode='local'
        """
    )

    assert res == "jack\njohn\n"

    res = instance.query(
        f"""
            SELECT t1.name,t2.second_name
            FROM {creation_expression} AS t1
                CROSS JOIN `{TABLE_NAME_LOCAL}` AS t2
            WHERE t1.tag < 10 AND t2.id < 20
            ORDER BY ALL
            SETTINGS
                object_storage_cluster='cluster_simple',
                object_storage_cluster_join_mode='local'
        """
    )

    assert res == "jack\tblack\njack\tsilver\njohn\tblack\njohn\tsilver\n"
