import logging

import pytest

from helpers.cluster import ClickHouseCluster
from test_storage_delta.test import (
    get_spark,
    write_delta_from_file,
    upload_directory,
    create_initial_data_file,
    randomize_table_name,
)
from test_storage_s3.test_sts import run_s3_mocks

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s3_with_environment_credentials",
            with_minio=True,
            env_variables={
                "AWS_ACCESS_KEY_ID": "aws",
                "AWS_SECRET_ACCESS_KEY": "aws123",
            },
            main_configs=[
                "configs/config.d/use_environment_credentials.xml",
            ],
            user_configs=[
                "configs/allow_server_credentials.xml",
            ],
        )

        sts = cluster.add_instance(
            name="sts.amazonaws.com",
            hostname="sts.amazonaws.com",
            image="clickhouse/python-bottle",
            tag="latest",
            stay_alive=True,
        )
        sts.stop_clickhouse(kill=True)
        cluster.spark_session = get_spark()

        logging.info("Starting cluster...")
        cluster.start()

        if int(cluster.instances["s3_with_environment_credentials"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip(
                "DeltaLake engine is not available"
            )

        logging.info("Cluster started")

        logging.info("S3 bucket created")
        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_sts_smoke(started_cluster):
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    TABLE_NAME = randomize_table_name("test_sts_smoke")
    bucket = started_cluster.minio_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    node_name = "s3_with_environment_credentials"
    instance = started_cluster.instances[node_name]
    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT toUInt64(number), toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=node_name,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert 100 == int(
        instance.query(
            f"""
       SELECT count() FROM deltaLake(
           'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}/',
            extra_credentials(role_arn = 'arn::role', role_session_name = 'miniorole'),
            SETTINGS allow_experimental_delta_kernel_rs=1)
       """
        )
    )
