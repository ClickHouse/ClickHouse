import logging
import os

import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster

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

    for address, dirs, files in os.walk(f"{SCRIPT_DIR}/test_table"):
        for name in files:
            started_cluster.minio_client.fput_object(bucket, os.path.join(SCRIPT_DIR, address, name), os.path.join(address, name))

    for obj in list(
        minio.list_objects(
            bucket,
            recursive=True,
        )
    ):
        logging.info(obj.name)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "dummy",
            with_minio=True
        )

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
    instance = started_cluster.instances["dummy"]
    bucket = started_cluster.minio_bucket

    create_query = f"""CREATE TABLE hudi ENGINE=Hudi('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/hudi', 'minio', 'minio123')"""

    run_query(instance, create_query)

def test_select_query():
    pass