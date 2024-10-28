import json
import logging
import os

import pytest

import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.mock_servers import start_mock_servers

MINIO_INTERNAL_PORT = 9001

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(started_cluster):
    # Allows read-write access for bucket without authorization.
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

    started_cluster.minio_restricted_bucket = "{}-with-auth".format(
        started_cluster.minio_bucket
    )
    if minio_client.bucket_exists(started_cluster.minio_restricted_bucket):
        minio_client.remove_bucket(started_cluster.minio_restricted_bucket)

    minio_client.make_bucket(started_cluster.minio_restricted_bucket)


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("mock_s3.py", "resolver", "8080"),
            ("unstable_server.py", "resolver", "8081"),
            ("echo.py", "resolver", "8082"),
            ("no_list_objects.py", "resolver", "8083"),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s3_with_invalid_environment_credentials",
            with_minio=True,
            env_variables={
                "AWS_ACCESS_KEY_ID": "aws",
                "AWS_SECRET_ACCESS_KEY": "aws123",
            },
            main_configs=[
                "configs/use_environment_credentials.xml",
                "configs/named_collections.xml",
            ],
            user_configs=["configs/users.xml"],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")
        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_with_invalid_environment_credentials(started_cluster):
    instance = started_cluster.instances["s3_with_invalid_environment_credentials"]

    for bucket, auth in [
        (started_cluster.minio_restricted_bucket, "'minio', 'minio123'"),
        (started_cluster.minio_bucket, "NOSIGN"),
    ]:
        instance.query(
            f"insert into function s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache4.jsonl', {auth}) select * from numbers(100) settings s3_truncate_on_insert=1"
        )

        with pytest.raises(helpers.client.QueryRuntimeException) as ei:
            instance.query(
                f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache4.jsonl')"
            )

            assert ei.value.returncode == 243
            assert "HTTP response code: 403" in ei.value.stderr

        assert (
            "100"
            == instance.query(
                f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache4.jsonl', {auth})"
            ).strip()
        )


def test_no_sign_named_collections(started_cluster):
    instance = started_cluster.instances["s3_with_invalid_environment_credentials"]

    bucket = started_cluster.minio_bucket

    instance.query(
        f"insert into function s3(s3_json_no_sign) select * from numbers(100) settings s3_truncate_on_insert=1"
    )

    with pytest.raises(helpers.client.QueryRuntimeException) as ei:
        instance.query(
            f"select count() from s3('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/test_cache4.jsonl')"
        )

        assert ei.value.returncode == 243
        assert "HTTP response code: 403" in ei.value.stderr

    assert "100" == instance.query(f"select count() from s3(s3_json_no_sign)").strip()
