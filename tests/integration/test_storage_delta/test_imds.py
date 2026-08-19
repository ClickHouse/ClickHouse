import glob
import json
import logging
import os
import random
import string
import time
import uuid
from datetime import datetime

import pytest
from delta import *
from deltalake.writer import write_deltalake
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.config_cluster import minio_access_key
from helpers.config_cluster import minio_secret_key
from test_storage_delta.test import (
    get_spark,
    write_delta_from_file,
    upload_directory,
    create_initial_data_file,
    randomize_table_name,
)
from helpers.s3_tools import (
    prepare_s3_bucket,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
METADATA_SERVER_HOSTNAME = "resolver"
METADATA_SERVER_PORT = 8080


def start_metadata_server(cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "metadata_servers")
    start_mock_servers(
        cluster,
        script_dir,
        [
            (
                "server_with_session_tokens.py",
                METADATA_SERVER_HOSTNAME,
                METADATA_SERVER_PORT,
            )
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node_with_session_token",
            with_minio=True,
            main_configs=[
                "configs/config.d/use_environment_credentials.xml",
            ],
            env_variables={
                "AWS_EC2_METADATA_SERVICE_ENDPOINT": f"{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}",
            },
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        start_metadata_server(cluster)

        cluster.spark_session = get_spark()
        start_metadata_server(cluster)

        yield cluster

    finally:
        cluster.shutdown()


def test_session_token(started_cluster):
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    TABLE_NAME = randomize_table_name("test_session_token")
    bucket = started_cluster.minio_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    instance = started_cluster.instances["node_with_session_token"]
    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT toUInt64(number), toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name="node_with_session_token",
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert 0 < int(
        instance.query(
            f"""
       SELECT count() FROM deltaLake(
           'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}/',
           SETTINGS allow_experimental_delta_kernel_rs=1)
       """
        )
    )

    expected_logs = [
        "Calling EC2MetadataService to get token",
        "Calling EC2MetadataService resource, /latest/meta-data/iam/security-credentials with token returned profile string myrole",
        "Calling EC2MetadataService resource resolver:8080/latest/meta-data/iam/security-credentials/myrole with token",
        "Successfully pulled credentials from EC2MetadataService with access key",
    ]

    instance.query("SYSTEM FLUSH LOGS")
    for expected_msg in expected_logs:
        assert instance.contains_in_log(
            "AWSEC2InstanceProfileConfigLoader: " + expected_msg
        )
