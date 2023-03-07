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
TABLE_NAME = "test_iceberg_table"
USER_FILES_PATH = "/ClickHouse/tests/integration/test_storage_iceberg/_instances/node1/database/user_files"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", with_minio=True)

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


def test_basic(started_cluster):
    instance = started_cluster.instances["node1"]

    data_path = f"/var/lib/clickhouse/user_files/{TABLE_NAME}.parquet"
    inserted_data = "SELECT number, toString(number) FROM numbers(100)"
    instance.query(
        f"INSERT INTO TABLE FUNCTION file('{data_path}') {inserted_data} FORMAT Parquet"
    )

    instance.exec_in_container(
        ["bash", "-c", "chmod 777 -R /var/lib/clickhouse/user_files"],
        user="root",
    )

    spark = get_spark()
    result_path = f"/{TABLE_NAME}_result"

    spark.read.load(f"file://{USER_FILES_PATH}/{TABLE_NAME}.parquet").writeTo(
        TABLE_NAME
    ).using("iceberg").create()

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    upload_directory(
        minio_client, bucket, "/iceberg_data/default/test_iceberg_table", ""
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=Iceberg('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/iceberg_data/default/test_iceberg_table/', 'minio', 'minio123')"""
    )
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )
