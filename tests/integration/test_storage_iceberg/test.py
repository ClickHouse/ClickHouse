import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import pyspark
import logging
import os
import json
import pytest
import time

from helpers.s3_tools import prepare_s3_bucket, upload_directory, get_file_contents

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
USER_FILES_PATH = os.path.join(SCRIPT_DIR, "./_instances/node1/database/user_files")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
        .master("local")
    )
    return builder.master("local").getOrCreate()


def write_iceberg_from_file(spark, path, table_name, mode="overwrite"):
    spark.read.load(f"file://{path}").writeTo(table_name).using("iceberg").create()


def write_iceberg_from_df(spark, df, table_name, mode="overwrite"):
    df.writeTo(table_name).using("iceberg").create()


def create_iceberg_table(node, table_name):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=Iceberg(s3, filename = '{table_name}/')"""
    )


def create_initial_data_file(node, query, table_name, compression_method="none"):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            s3_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    result_path = f"{USER_FILES_PATH}/{table_name}.parquet"
    return result_path


def test_single_iceberg_file(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_single_iceberg_file"

    inserted_data = "SELECT number, toString(number) FROM numbers(100)"
    parquet_data_path = create_initial_data_file(instance, inserted_data, TABLE_NAME)
    write_iceberg_from_file(spark, parquet_data_path, TABLE_NAME)

    files = upload_directory(
        minio_client, bucket, f"/iceberg_data/default/{TABLE_NAME}/", ""
    )

    create_iceberg_table(instance, TABLE_NAME)
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


def test_multiple_iceberg_files(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_multiple_iceberg_files"
