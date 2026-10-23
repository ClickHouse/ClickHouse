import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    additional_upload_directory,
    default_download_directory,
    write_iceberg_from_df,
    generate_data,
    get_uuid_str,
    get_creation_expression,
    create_iceberg_table
)

import logging
import time
import uuid
import pyarrow.parquet as pq
from helpers.config_cluster import minio_secret_key


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
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

        if storage_type == "local":
            # For local storage we need to upload data to each node
            for node_name, replica in started_cluster_iceberg_with_spark.instances.items():
                if node_name == "node1":
                    continue
                additional_upload_directory(
                    started_cluster_iceberg_with_spark,
                    node_name,
                    storage_type,
                    f"/iceberg_data/default/{TABLE_NAME}/",
                    f"/iceberg_data/default/{TABLE_NAME}/",
                )

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
    instance.query(f"INSERT INTO {TABLE_NAME_2} SELECT * FROM {table_function_expr_cluster};", settings={"allow_insert_into_iceberg": 1})

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

    pq_file = ""
    files = add_df(mode="overwrite")
    for i in range(1, 5 * len(started_cluster_iceberg_with_spark.instances)):
        files = add_df(mode="append")
        for file in files:
            if file[-7:] == 'parquet':
                pq_file = file

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

    if storage_type != "s3":
        return
    table_function_expr_s3_cluster = f"s3Cluster('cluster_simple', 'http://minio1:9001/root/{pq_file}', 'minio', '{minio_secret_key}', 'Parquet')"
    buffers_count_with_splitted_tasks = get_buffers_count(lambda: instance.query(f"SELECT * FROM {table_function_expr_s3_cluster} ORDER BY ALL SETTINGS input_format_parquet_use_native_reader_v3={input_format_parquet_use_native_reader_v3},cluster_table_function_split_granularity='bucket', cluster_table_function_buckets_batch_size={cluster_table_function_buckets_batch_size}").strip().split())
    buffers_count_default = get_buffers_count(lambda: instance.query(f"SELECT * FROM {table_function_expr_s3_cluster} ORDER BY ALL SETTINGS input_format_parquet_use_native_reader_v3={input_format_parquet_use_native_reader_v3}, cluster_table_function_buckets_batch_size={cluster_table_function_buckets_batch_size}").strip().split())
    if buffers_count_with_splitted_tasks != 0:
        assert buffers_count_with_splitted_tasks >= buffers_count_default

@pytest.mark.parametrize("storage_type", ["s3"])
def test_empty_parquet_file(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    format_version = '2'
    TABLE_NAME = "test_empty_parquet_file_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1);", settings={"allow_insert_into_iceberg":1})

    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )

    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(files) > 0

    pq_file = ""
    for file in files:
        if file[-7:] == 'parquet':
            pq_file = file
            break

    assert len(pq_file) > 0, files
    pq_file = '/' + pq_file
    schema = pq.read_schema(pq_file)
    empty_table = schema.empty_table()
    with pq.ParquetWriter(pq_file, schema) as writer:
        pass
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster} ORDER BY ALL SETTINGS cluster_table_function_split_granularity='bucket'")
    )

    assert select_cluster == ''
