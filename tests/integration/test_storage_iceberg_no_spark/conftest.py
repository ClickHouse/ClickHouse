import pytest
import logging
import pyspark


from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    S3Downloader,
    LocalDownloader,
    prepare_s3_bucket,
)

@pytest.fixture(scope="package")
def started_cluster_iceberg_no_spark():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/metadata_log.xml",
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
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/no_metadata_log.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

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
