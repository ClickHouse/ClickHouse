import gzip
import json
import logging
import os
import io
import random
import threading
import time

import helpers.client
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
    # Allows read-write access for bucket without authorization.
    bucket_read_write_policy = {"Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:GetBucketLocation",
                                        "Resource": "arn:aws:s3:::root"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:ListBucket",
                                        "Resource": "arn:aws:s3:::root"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:GetObject",
                                        "Resource": "arn:aws:s3:::root/*"
                                    },
                                    {
                                        "Sid": "",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "s3:PutObject",
                                        "Resource": "arn:aws:s3:::root/*"
                                    }
                                ]}

    minio_client = cluster.minio_client
    minio_client.set_bucket_policy(cluster.minio_bucket, json.dumps(bucket_read_write_policy))

    cluster.minio_restricted_bucket = "{}-with-auth".format(cluster.minio_bucket)
    if minio_client.bucket_exists(cluster.minio_restricted_bucket):
        minio_client.remove_bucket(cluster.minio_restricted_bucket)

    minio_client.make_bucket(cluster.minio_restricted_bucket)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, name="redirect")
        cluster.add_instance("dummy", with_minio=True, main_configs=["configs/defaultS3.xml"])
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")
        run_s3_mock(cluster)

        yield cluster
    finally:
        cluster.shutdown()

def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result

def run_s3_mock(cluster):
    logging.info("Starting s3 mock")
    container_id = cluster.get_container_id('resolver')
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "s3_mock", "mock_s3.py"), "mock_s3.py")
    cluster.exec_in_container(container_id, ["python", "mock_s3.py"], detach=True)

    # Wait for S3 mock start
    for attempt in range(10):
        ping_response = cluster.exec_in_container(cluster.get_container_id('resolver'),
                                                  ["curl", "-s", "http://resolver:8080/"], nothrow=True)
        if ping_response != 'OK':
            if attempt == 9:
                assert ping_response == 'OK', 'Expected "OK", but got "{}"'.format(ping_response)
            else:
                time.sleep(1)
        else:
            break

    logging.info("S3 mock started")

def test_infinite_redirect(started_cluster):
    bucket = "redirected"
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    filename = "test.csv"
    get_query = "select * from s3('http://resolver:8080/{bucket}/{file}', 'CSV', '{table_format}')".format(
        bucket=bucket,
        file=filename,
        table_format=table_format)
    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    exception_raised = False
    try:
        run_query(instance, get_query)
    except Exception as e:
        assert str(e).find("Too many redirects while trying to access") != -1
        exception_raised = True
    finally:
        assert exception_raised