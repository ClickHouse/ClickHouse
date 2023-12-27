import logging
import os
import json
import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def prepare_s3_bucket(started_cluster):
    bucket_read_write_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::root",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::root/*",
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": "arn:aws:s3:::root/*",
            },
        ],
    }

    minio_client = started_cluster.minio_client
    minio_client.set_bucket_policy(
        started_cluster.minio_bucket, json.dumps(bucket_read_write_policy)
    )


def upload_test_table(started_cluster):
    bucket = started_cluster.minio_bucket

    for address, dirs, files in os.walk(SCRIPT_DIR + "/taxis"):
        address_without_prefix = address[len(SCRIPT_DIR) :]

        for name in files:
            started_cluster.minio_client.fput_object(
                bucket,
                os.path.join(address_without_prefix, name),
                os.path.join(address, name),
            )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("main_server", with_minio=True)

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        upload_test_table(cluster)
        logging.info("Test table uploaded")

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def test_create_query(started_cluster):
    instance = started_cluster.instances["main_server"]
    bucket = started_cluster.minio_bucket

    create_query = f"""CREATE TABLE iceberg ENGINE=Iceberg('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/taxis/', 'minio', 'minio123')"""

    run_query(instance, create_query)


def test_select_query(started_cluster):
    instance = started_cluster.instances["main_server"]
    bucket = started_cluster.minio_bucket
    columns = [
        "vendor_id",
        "trip_id",
        "trip_distance",
        "fare_amount",
        "store_and_fwd_flag",
    ]

    # create query in case table doesn't exist
    create_query = f"""CREATE TABLE IF NOT EXISTS iceberg ENGINE=Iceberg('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/taxis/', 'minio', 'minio123')"""

    run_query(instance, create_query)

    select_query = "SELECT {} FROM iceberg FORMAT TSV"
    select_table_function_query = "SELECT {col} FROM iceberg('http://{ip}:{port}/{bucket}/taxis/', 'minio', 'minio123') FORMAT TSV"

    for column_name in columns:
        result = run_query(instance, select_query.format(column_name)).splitlines()
        assert len(result) > 0

    for column_name in columns:
        result = run_query(
            instance,
            select_table_function_query.format(
                col=column_name,
                ip=started_cluster.minio_ip,
                port=started_cluster.minio_port,
                bucket=bucket,
            ),
        ).splitlines()
        assert len(result) > 0


def test_describe_query(started_cluster):
    instance = started_cluster.instances["main_server"]
    bucket = started_cluster.minio_bucket
    result = instance.query(
        f"DESCRIBE iceberg('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/taxis/', 'minio', 'minio123') FORMAT TSV",
    )

    assert result == TSV(
        [
            ["vendor_id", "Nullable(Int64)"],
            ["trip_id", "Nullable(Int64)"],
            ["trip_distance", "Nullable(Float32)"],
            ["fare_amount", "Nullable(Float64)"],
            ["store_and_fwd_flag", "Nullable(String)"],
        ]
    )
