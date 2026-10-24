import logging

import pytest

logging.getLogger("py4j").setLevel(logging.ERROR)  # before import pyspark; prevents lots of log spam

from helpers.cluster import ClickHouseCluster
from helpers.paimon_utils import get_spark_paimon
from helpers.s3_tools import (
    AzureUploader,
    S3Uploader,
    prepare_s3_bucket,
)
from helpers.spark_tools import ResilientSparkSession


@pytest.fixture(scope="package")
def started_cluster_paimon_with_spark():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/disks.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_azurite=True,
            with_zookeeper=True,
            macros={"replica": "r1"},
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/paimon_cache_small.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_zookeeper=True,
            macros={"replica": "r2"},
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark_paimon(cluster.instances_dir)
        )
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.azure_container_name = "mycontainer"
        cluster.container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )
        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        yield cluster

    finally:
        cluster.shutdown()
