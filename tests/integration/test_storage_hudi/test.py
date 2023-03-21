import logging
import pytest
import os
import json

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.s3_tools import prepare_s3_bucket, upload_directory, get_file_contents

import pyspark

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TABLE_NAME = "test_hudi_table"
USER_FILES_PATH = os.path.join(SCRIPT_DIR, "./_instances/node1/database/user_files")


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
            "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0",
        )
        .config(
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.catalog.local", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
        )
        .config("spark.driver.memory", "20g")
        .master("local")
    )
    return builder.master("local").getOrCreate()


def write_hudi(spark, path, result_path):
    spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
    spark.read.load(f"file://{path}").write.mode("overwrite").option(
        "compression", "none"
    ).option("compression", "none").format("hudi").option(
        "hoodie.table.name", TABLE_NAME
    ).option(
        "hoodie.datasource.write.partitionpath.field", "partitionpath"
    ).option(
        "hoodie.datasource.write.table.name", TABLE_NAME
    ).option(
        "hoodie.datasource.write.operation", "insert_overwrite"
    ).option(
        "hoodie.parquet.compression.codec", "snappy"
    ).option(
        "hoodie.hfile.compression.algorithm", "uncompressed"
    ).option(
        "hoodie.datasource.write.recordkey.field", "a"
    ).option(
        "hoodie.datasource.write.precombine.field", "a"
    ).save(
        result_path
    )


def test_basic(started_cluster):
    instance = started_cluster.instances["node1"]

    data_path = f"/var/lib/clickhouse/user_files/{TABLE_NAME}.parquet"
    inserted_data = "SELECT number, toString(number) FROM numbers(100)"
    instance.query(
        f"INSERT INTO TABLE FUNCTION file('{data_path}', 'Parquet', 'a Int32, b String') SETTINGS output_format_parquet_compression_method='none' {inserted_data} FORMAT Parquet"
    )

    data_path = f"{USER_FILES_PATH}/{TABLE_NAME}.parquet"
    result_path = f"/{TABLE_NAME}_result"

    spark = get_spark()
    write_hudi(spark, data_path, result_path)

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    paths = upload_directory(minio_client, bucket, result_path, "")
    assert len(paths) == 1
    assert paths[0].endswith(".parquet")

    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=Hudi('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{TABLE_NAME}_result/__HIVE_DEFAULT_PARTITION__/', 'minio', 'minio123')"""
    )
    assert instance.query(f"SELECT a, b FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )
