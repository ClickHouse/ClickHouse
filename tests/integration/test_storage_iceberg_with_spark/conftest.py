import os.path as p
import random

import pytest
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)  # before import pyspark; prevents lots of log spam
import pyspark

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Downloader,
    S3Uploader,
    LocalDownloader,
    prepare_s3_bucket,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_storage_iceberg_with_spark")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/var/lib/clickhouse/user_files/iceberg_data")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


@pytest.fixture(scope="package")
def started_cluster_iceberg_with_spark():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)

        filesystem_cache_name = f"cache_{random.randint(10000, 99999)}"
        cluster.filesystem_cache_name = filesystem_cache_name
        filesystem_cache_config_path = p.abspath(
            p.join( p.dirname(cluster.base_path), f'configs/config.d/{filesystem_cache_name}.xml'))
        logging.info(filesystem_cache_config_path)
        with open(filesystem_cache_config_path, "w") as f:
            f.write(f"""
<clickhouse>
  <filesystem_caches>
    <{filesystem_cache_name}>
      <max_size>1Gi</max_size>
      <path>{filesystem_cache_name}</path>
    </{filesystem_cache_name}>
  </filesystem_caches>
</clickhouse>
""")
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/metadata_log.xml",
                filesystem_cache_config_path
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/metadata_log.xml",
                filesystem_cache_config_path
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/metadata_log.xml",
                filesystem_cache_config_path
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.azure_container_name = "mycontainer"

        cluster.blob_service_client = cluster.blob_service_client

        container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )

        cluster.container_client = container_client

        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])
        cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)

        yield cluster

    finally:
        cluster.shutdown()
