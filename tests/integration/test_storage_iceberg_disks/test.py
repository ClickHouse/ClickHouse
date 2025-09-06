import glob
import json
import logging
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

import pyspark
import pytest
from azure.storage.blob import BlobServiceClient
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    list_s3_objects,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
    default_download_directory,
    execute_spark_query_general,
    get_creation_expression,
    write_iceberg_from_df,
    generate_data,
    create_iceberg_table,
    drop_iceberg_table,
    check_schema_and_data,
    get_uuid_str,
    check_validity_and_get_prunned_files_general,
    get_last_snapshot
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )
    return builder.master("local").getOrCreate()

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_amd.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = get_spark()
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

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_single_iceberg_file(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        f"test_single_iceberg_file"
    )

    try:
        write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)
    except:
        pass

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"CREATE TABLE {TABLE_NAME}_{storage_type} ENGINE=Iceberg() SETTINGS iceberg_disk_name = 'disk_{storage_type}'")

    assert instance.query(f"SELECT * FROM {TABLE_NAME}_{storage_type}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(f"CREATE TABLE {TABLE_NAME}_{storage_type}_2 ENGINE=Iceberg('Parquet') SETTINGS iceberg_disk_name = 'disk_{storage_type}'")

    assert instance.query(f"SELECT * FROM {TABLE_NAME}_{storage_type}_2") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    assert instance.query(f"SELECT * FROM iceberg() SETTINGS iceberg_disk_name = 'disk_{storage_type}'") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )
