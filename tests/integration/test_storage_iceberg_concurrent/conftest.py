import pytest
import logging
import pyspark


from helpers.cluster import ClickHouseCluster, minio_access_key, minio_secret_key
from helpers.s3_tools import (
    prepare_s3_bucket,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config

def get_spark(cluster : ClickHouseCluster):

    builder = (
        pyspark.sql.SparkSession.builder \
            .appName("IcebergS3Example") \
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
                "configs/config.d/named_collections.xml",
                "configs/config.d/log_level_override.xml",
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

        yield cluster

    finally:
        cluster.shutdown()
