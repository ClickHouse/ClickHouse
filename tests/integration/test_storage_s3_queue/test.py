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
from helpers.network import PartitionManager
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import exec_query_with_retry
from helpers.s3_tools import prepare_s3_bucket

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
)


MINIO_INTERNAL_PORT = 9001

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def put_s3_file_content(started_cluster, bucket, filename, data):
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(bucket, filename, buf, len(data))


# Returns content of given S3 file as string.
def get_s3_file_content(started_cluster, bucket, filename, decode=True):
    # type: (ClickHouseCluster, str, str, bool) -> str

    data = started_cluster.minio_client.get_object(bucket, filename)
    data_str = b""
    for chunk in data.stream():
        data_str += chunk
    if decode:
        return data_str.decode()
    return data_str


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "instance",
            user_configs=["configs/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            main_configs=[
                "configs/defaultS3.xml",
                "configs/named_collections.xml"
            ],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        # logging.info("S3 bucket created")
        # run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def test_get_file(started_cluster):
    auth = "'minio','minio123',"
    bucket = started_cluster.minio_restricted_bucket
    instance = started_cluster.instances["instance"]
    table_format = "column1 UInt32, column2 UInt32, column3 UInt32"
    values = [
        [12549, 2463, 19893],
        [64021, 38652, 66703],
        [81611, 39650, 83516],
        [11079, 59507, 61546],
        [51764, 69952, 6876],
        [41165, 90293, 29095],
        [40167, 78432, 48309],
        [81629, 81327, 11855],
        [55852, 21643, 98507],
        [6738, 54643, 41155],
    ]
    values_csv = (
            "\n".join((",".join(map(str, row)) for row in values)) + "\n"
    ).encode()
    filename = f"test.csv"
    put_s3_file_content(started_cluster, bucket, filename, values_csv)

    instance.query(
        f"create table test ({table_format}) engine=S3Queue('http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/*', {auth}'CSV') SETTINGS mode = 'unordered', keeper_path = '/clickhouse/testing'"
    )

    get_query = f"SELECT * FROM test"
    assert [
               list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
           ] == values

    get_query = f"SELECT * FROM test"
    assert [
           list(map(int, l.split())) for l in run_query(instance, get_query).splitlines()
       ] == []