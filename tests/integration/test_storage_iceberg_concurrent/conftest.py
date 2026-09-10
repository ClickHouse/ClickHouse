import pytest
import logging
import pyspark
import os
import subprocess


from helpers.cluster import ClickHouseCluster, minio_access_key, minio_secret_key
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    prepare_s3_bucket,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config

def check_spark(spark):
    p = subprocess.run(["echo", "hello world!"], capture_output=True, text=True)
    logging.info(f"Java version: {p.stdout}")


    spark.sql(
        """
        DROP DATABASE IF EXISTS spark_catalog.db CASCADE
        """
    )

    spark.sql(
        """
        CREATE DATABASE spark_catalog.db
        """
    )

    spark.sql(
    """
        CREATE TABLE IF NOT EXISTS spark_catalog.db.lol2 (order_number bigint) USING iceberg OPTIONS ('format-version'='1');
    """
    )

    spark.sql(
    """
        INSERT INTO spark_catalog.db.lol2 (order_number) VALUES (123), (456), (789);
    """
    )

    logging.info(f"Dataframe: {spark.sql('SELECT * FROM spark_catalog.db.lol2').show()}")

def get_spark(cluster : ClickHouseCluster):
    iceberg_version = "1.4.3"
    spark_version = "3.5.1"
    hadoop_aws_version = "3.3.4"
    jdk_bundle = "1.12.262"

    builder = (
        pyspark.sql.SparkSession.builder \
            .appName("IcebergS3Example") \
            .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
            .config("spark.jars.packages",
            f'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version},'
            f'org.apache.spark:spark-avro_2.12:{spark_version},'
            f'org.apache.hadoop:hadoop-aws:{hadoop_aws_version},'
            f'com.amazonaws:aws-java-sdk-bundle:{jdk_bundle}')\
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://root/var/lib/clickhouse/user_files/iceberg_data") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{cluster.minio_ip}:{cluster.minio_port}/") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .master("local")
            )

    props_path = write_spark_log_config(cluster.instances_dir)
    builder = builder.config(
        "spark.driver.extraJavaOptions",
        f"-Dlog4j2.configurationFile=file:{props_path}",
    )

    return builder.getOrCreate()

@pytest.fixture(scope="package")
def started_cluster_iceberg():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
            mem_limit='15g',
            cpu_limit=False,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)

        cluster.spark_session = ResilientSparkSession(lambda: get_spark(cluster))

        # check_spark(cluster.spark_session)

        yield cluster

    finally:
        cluster.shutdown()
