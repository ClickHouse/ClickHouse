import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    get_uuid_str,
    get_creation_expression,
    create_iceberg_table
)

import logging
import time
import uuid

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_cluster_table_function(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_iceberg_cluster_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def add_df(mode):
        write_iceberg_from_df(
            spark,
            generate_data(spark, 0, 100),
            TABLE_NAME,
            mode=mode,
            format_version=format_version,
        )

        files = default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        logging.info(f"Adding another dataframe. result files: {files}")

        return files

    files = add_df(mode="overwrite")
    for i in range(1, len(started_cluster_iceberg_with_spark.instances)):
        files = add_df(mode="append")

    logging.info(f"Setup complete. files: {files}")
    assert len(files) == 5 + 4 * (len(started_cluster_iceberg_with_spark.instances) - 1)

    clusters = instance.query(f"SELECT * FROM system.clusters")
    logging.info(f"Clusters setup: {clusters}")

    # Regular Query only node1
    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr}").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )
    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster}").strip().split()
    )

    # Simple size check
    assert len(select_regular) == 600
    assert len(select_cluster) == 600

    # Actual check
    assert select_cluster == select_regular

    # Check query_log
    for replica in started_cluster_iceberg_with_spark.instances.values():
        replica.query("SYSTEM FLUSH LOGS")

    for node_name, replica in started_cluster_iceberg_with_spark.instances.items():
        cluster_secondary_queries = (
            replica.query(
                f"""
                SELECT query, type, is_initial_query, read_rows, read_bytes FROM system.query_log
                WHERE
                    type = 'QueryStart' AND
                    positionCaseInsensitive(query, '{storage_type}Cluster') != 0 AND
                    position(query, '{TABLE_NAME}') != 0 AND
                    position(query, 'system.query_log') = 0 AND
                    NOT is_initial_query
            """
            )
            .strip()
            .split("\n")
        )

        logging.info(
            f"[{node_name}] cluster_secondary_queries: {cluster_secondary_queries}"
        )
        assert len(cluster_secondary_queries) == 1

    # write 3 times
    assert int(instance.query(f"SELECT count() FROM {table_function_expr_cluster}")) == 100 * 3

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_writes_cluster_table_function(started_cluster_iceberg_with_spark, format_version, storage_type):

    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_iceberg_cluster_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    TABLE_NAME_2 = TABLE_NAME + "_2"
    def add_df(mode):
        write_iceberg_from_df(
            spark,
            generate_data(spark, 0, 100),
            TABLE_NAME,
            mode=mode,
            format_version=format_version,
        )

        files = default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        logging.info(f"Adding another dataframe. result files: {files}")

        return files

    files = add_df(mode="overwrite")
    for i in range(1, len(started_cluster_iceberg_with_spark.instances)):
        files = add_df(mode="append")

    logging.info(f"Setup complete. files: {files}")
    assert len(files) == 5 + 4 * (len(started_cluster_iceberg_with_spark.instances) - 1)

    clusters = instance.query(f"SELECT * FROM system.clusters")
    logging.info(f"Clusters setup: {clusters}")

    # Regular Query only node1
    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr}").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )
    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster}").strip().split()
    )

    # Simple size check
    assert len(select_regular) == 600
    assert len(select_cluster) == 600

    create_iceberg_table(storage_type, instance, TABLE_NAME_2, started_cluster_iceberg_with_spark, "(a Int32, b String)", format_version)
    instance.query(f"INSERT INTO {TABLE_NAME_2} SELECT * FROM {table_function_expr_cluster};", settings={"allow_experimental_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {table_function_expr_cluster}") == instance.query(f"SELECT * FROM {TABLE_NAME_2}")

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
@pytest.mark.parametrize("cluster_table_function_buckets_batch_size", [0, 100, 1000])
@pytest.mark.parametrize("input_format_parquet_use_native_reader_v3", [0, 1])
def test_cluster_table_function_split_by_row_groups(started_cluster_iceberg_with_spark, format_version, storage_type, cluster_table_function_buckets_batch_size,input_format_parquet_use_native_reader_v3):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_iceberg_cluster_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def add_df(mode):
        write_iceberg_from_df(
            spark,
            generate_data(spark, 0, 100000),
            TABLE_NAME,
            mode=mode,
            format_version=format_version,
        )

        files = default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        logging.info(f"Adding another dataframe. result files: {files}")

        return files

    files = add_df(mode="overwrite")
    for i in range(1, 5 * len(started_cluster_iceberg_with_spark.instances)):
        files = add_df(mode="append")

    clusters = instance.query(f"SELECT * FROM system.clusters")
    logging.info(f"Clusters setup: {clusters}")

    # Regular Query only node1
    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr} ORDER BY ALL").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )
    instance.query("SYSTEM FLUSH LOGS")

    def get_buffers_count(func):
        buffers_count_before = int(
            instance.query(
                f"SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE type = 'QueryFinish'"
            )
        )

        func()
        instance.query("SYSTEM FLUSH LOGS")
        buffers_count = int(
            instance.query(
                f"SELECT sum(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE type = 'QueryFinish'"
            )
        )
        return buffers_count - buffers_count_before

    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster} ORDER BY ALL SETTINGS input_format_parquet_use_native_reader_v3={input_format_parquet_use_native_reader_v3},cluster_table_function_split_granularity='bucket', cluster_table_function_buckets_batch_size={cluster_table_function_buckets_batch_size}").strip().split()
    )

    # Simple size check
    assert len(select_cluster) == len(select_regular)
    # Actual check
    assert select_cluster == select_regular

    buffers_count_with_splitted_tasks = get_buffers_count(lambda: instance.query(f"SELECT * FROM {table_function_expr_cluster} ORDER BY ALL SETTINGS input_format_parquet_use_native_reader_v3={input_format_parquet_use_native_reader_v3},cluster_table_function_split_granularity='bucket', cluster_table_function_buckets_batch_size={cluster_table_function_buckets_batch_size}").strip().split())
    buffers_count_default = get_buffers_count(lambda: instance.query(f"SELECT * FROM {table_function_expr_cluster} ORDER BY ALL SETTINGS input_format_parquet_use_native_reader_v3={input_format_parquet_use_native_reader_v3}, cluster_table_function_buckets_batch_size={cluster_table_function_buckets_batch_size}").strip().split())
    assert buffers_count_with_splitted_tasks > buffers_count_default
