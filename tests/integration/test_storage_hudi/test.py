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

    for address, dirs, files in os.walk(SCRIPT_DIR + "/test_table"):
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

    create_query = f"""CREATE TABLE hudi ENGINE=Hudi('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/test_table/', 'minio', 'minio123')"""

    run_query(instance, create_query)


def test_select_query(started_cluster):
    instance = started_cluster.instances["main_server"]
    bucket = started_cluster.minio_bucket
    columns = [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "begin_lat",
        "begin_lon",
        "driver",
        "end_lat",
        "end_lon",
        "fare",
        "partitionpath",
        "rider",
        "ts",
        "uuid",
    ]

    # create query in case table doesn't exist
    create_query = f"""CREATE TABLE IF NOT EXISTS hudi ENGINE=Hudi('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/test_table/', 'minio', 'minio123')"""

    run_query(instance, create_query)

    select_query = "SELECT {} FROM hudi FORMAT TSV"

    select_table_function_query = "SELECT {col} FROM hudi('http://{ip}:{port}/{bucket}/test_table/', 'minio', 'minio123') FORMAT TSV"

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

    # test if all partition paths is presented in result
    distinct_select_query = (
        "SELECT DISTINCT partitionpath FROM hudi ORDER BY partitionpath FORMAT TSV"
    )

    distinct_select_table_function_query = "SELECT DISTINCT partitionpath FROM hudi('http://{ip}:{port}/{bucket}/test_table/', 'minio', 'minio123') ORDER BY partitionpath FORMAT TSV"

    result = run_query(instance, distinct_select_query)
    result_table_function = run_query(
        instance,
        distinct_select_table_function_query.format(
            ip=started_cluster.minio_ip, port=started_cluster.minio_port, bucket=bucket
        ),
    )
    expected = [
        "americas/brazil/sao_paulo",
        "americas/united_states/san_francisco",
        "asia/india/chennai",
    ]

    assert TSV(result) == TSV(expected)
    assert TSV(result_table_function) == TSV(expected)


def test_describe_query(started_cluster):
    instance = started_cluster.instances["main_server"]
    bucket = started_cluster.minio_bucket
    result = instance.query(
        f"DESCRIBE hudi('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/test_table/', 'minio', 'minio123') FORMAT TSV",
    )

    assert result == TSV(
        [
            ["_hoodie_commit_time", "Nullable(String)"],
            ["_hoodie_commit_seqno", "Nullable(String)"],
            ["_hoodie_record_key", "Nullable(String)"],
            ["_hoodie_partition_path", "Nullable(String)"],
            ["_hoodie_file_name", "Nullable(String)"],
            ["begin_lat", "Nullable(Float64)"],
            ["begin_lon", "Nullable(Float64)"],
            ["driver", "Nullable(String)"],
            ["end_lat", "Nullable(Float64)"],
            ["end_lon", "Nullable(Float64)"],
            ["fare", "Nullable(Float64)"],
            ["partitionpath", "Nullable(String)"],
            ["rider", "Nullable(String)"],
            ["ts", "Nullable(Int64)"],
            ["uuid", "Nullable(String)"],
        ]
    )
