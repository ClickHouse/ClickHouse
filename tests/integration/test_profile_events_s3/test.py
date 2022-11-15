import logging
import re

import pytest
import requests
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/log.xml",
                "configs/query_log.xml",
                "configs/ssl_conf.xml",
            ],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


init_list = {
    "ReadBufferFromS3Bytes": 0,
    "ReadBufferFromS3Microseconds": 0,
    "ReadBufferFromS3RequestsErrors": 0,
    "WriteBufferFromS3Bytes": 0,
    "S3ReadMicroseconds": 0,
    "S3ReadRequestsCount": 0,
    "S3ReadRequestsErrorsTotal": 0,
    "S3ReadRequestsErrors503": 0,
    "S3ReadRequestsRedirects": 0,
    "S3WriteMicroseconds": 0,
    "S3WriteRequestsCount": 0,
    "S3WriteRequestsErrorsTotal": 0,
    "S3WriteRequestsErrors503": 0,
    "S3WriteRequestsRedirects": 0,
    "DiskS3ReadMicroseconds": 0,
    "DiskS3ReadRequestsCount": 0,
    "DiskS3ReadRequestsErrorsTotal": 0,
    "DiskS3ReadRequestsErrors503": 0,
    "DiskS3ReadRequestsRedirects": 0,
    "DiskS3WriteMicroseconds": 0,
    "DiskS3WriteRequestsCount": 0,
    "DiskS3WriteRequestsErrorsTotal": 0,
    "DiskS3WriteRequestsErrors503": 0,
    "DiskS3WriteRequestsRedirects": 0,
    "S3DeleteObjects": 0,
    "S3CopyObject": 0,
    "S3ListObjects": 0,
    "S3HeadObject": 0,
    "S3CreateMultipartUpload": 0,
    "S3UploadPartCopy": 0,
    "S3UploadPart": 0,
    "S3AbortMultipartUpload": 0,
    "S3CompleteMultipartUpload": 0,
    "S3PutObject": 0,
    "S3GetObject": 0,
    "DiskS3DeleteObjects": 0,
    "DiskS3CopyObject": 0,
    "DiskS3ListObjects": 0,
    "DiskS3HeadObject": 0,
    "DiskS3CreateMultipartUpload": 0,
    "DiskS3UploadPartCopy": 0,
    "DiskS3UploadPart": 0,
    "DiskS3AbortMultipartUpload": 0,
    "DiskS3CompleteMultipartUpload": 0,
    "DiskS3PutObject": 0,
    "DiskS3GetObject": 0,
}


def get_s3_events(instance):
    result = init_list.copy()
    events = instance.query(
        "SELECT event, value FROM system.events WHERE event LIKE '%S3%'"
    ).split("\n")
    for event in events:
        ev = event.split("\t")
        if len(ev) == 2:
            result[ev[0]] = int(ev[1])
    return result


def get_minio_stat(cluster):
    result = {
        "get_requests": 0,
        "set_requests": 0,
        "errors": 0,
        "rx_bytes": 0,
        "tx_bytes": 0,
    }
    stat = requests.get(
        url="http://{}:{}/minio/prometheus/metrics".format(
            cluster.minio_ip, cluster.minio_port
        )
    ).text.split("\n")
    for line in stat:
        x = re.search(r"s3_requests_total(\{.*\})?\s(\d+)(\s.*)?", line)
        if x != None:
            y = re.search('.*api="(get|list|head|select).*', x.group(1))
            if y != None:
                result["get_requests"] += int(x.group(2))
            else:
                result["set_requests"] += int(x.group(2))
        x = re.search(r"s3_errors_total(\{.*\})?\s(\d+)(\s.*)?", line)
        if x != None:
            result["errors"] += int(x.group(2))
        x = re.search(r"s3_rx_bytes_total(\{.*\})?\s([\d\.e\+\-]+)(\s.*)?", line)
        if x != None:
            result["tx_bytes"] += float(x.group(2))
        x = re.search(r"s3_tx_bytes_total(\{.*\})?\s([\d\.e\+\-]+)(\s.*)?", line)
        if x != None:
            result["rx_bytes"] += float(x.group(2))
    return result


def get_query_stat(instance, hint):
    result = init_list.copy()
    instance.query("SYSTEM FLUSH LOGS")
    events = instance.query(
        """
        SELECT ProfileEvents.keys, ProfileEvents.values
        FROM system.query_log
        ARRAY JOIN ProfileEvents
        WHERE type != 1 AND query LIKE '%{}%'
        """.format(
            hint.replace("'", "\\'")
        )
    ).split("\n")
    for event in events:
        ev = event.split("\t")
        if len(ev) == 2:
            if "S3" in ev[0]:
                result[ev[0]] += int(ev[1])
    return result


def get_minio_size(cluster):
    minio = cluster.minio_client
    size = 0
    for obj_level1 in minio.list_objects(
        cluster.minio_bucket, prefix="data/", recursive=True
    ):
        size += obj_level1.size
    return size


def test_profile_events(cluster):
    instance = cluster.instances["node"]

    instance.query("SYSTEM FLUSH LOGS")

    instance.query("DROP TABLE IF EXISTS test_s3.test_s3")
    instance.query("DROP DATABASE IF EXISTS test_s3")
    instance.query("CREATE DATABASE IF NOT EXISTS test_s3")

    metrics0 = get_s3_events(instance)
    minio0 = get_minio_stat(cluster)

    query1 = "CREATE TABLE test_s3.test_s3 (key UInt32, value UInt32) ENGINE=MergeTree PRIMARY KEY key ORDER BY key SETTINGS storage_policy = 's3'"
    instance.query(query1)

    size1 = get_minio_size(cluster)
    metrics1 = get_s3_events(instance)
    minio1 = get_minio_stat(cluster)

    assert (
        metrics1["S3ReadRequestsCount"] - metrics0["S3ReadRequestsCount"]
        == minio1["get_requests"] - minio0["get_requests"] - 1
    )  # 1 from get_minio_size
    assert (
        metrics1["S3WriteRequestsCount"] - metrics0["S3WriteRequestsCount"]
        == minio1["set_requests"] - minio0["set_requests"]
    )
    stat1 = get_query_stat(instance, query1)
    for metric in stat1:
        assert stat1[metric] == metrics1[metric] - metrics0[metric]
    assert (
        metrics1["WriteBufferFromS3Bytes"] - metrics0["WriteBufferFromS3Bytes"] == size1
    )

    query2 = "INSERT INTO test_s3.test_s3 VALUES"
    instance.query(query2 + " (1,1)")

    size2 = get_minio_size(cluster)
    metrics2 = get_s3_events(instance)
    minio2 = get_minio_stat(cluster)

    assert (
        metrics2["S3ReadRequestsCount"] - metrics1["S3ReadRequestsCount"]
        == minio2["get_requests"] - minio1["get_requests"] - 1
    )  # 1 from get_minio_size
    assert (
        metrics2["S3WriteRequestsCount"] - metrics1["S3WriteRequestsCount"]
        == minio2["set_requests"] - minio1["set_requests"]
    )

    stat2 = get_query_stat(instance, query2)

    for metric in stat2:
        assert stat2[metric] == metrics2[metric] - metrics1[metric]

    assert (
        metrics2["WriteBufferFromS3Bytes"] - metrics1["WriteBufferFromS3Bytes"]
        == size2 - size1
    )

    query3 = "SELECT * from test_s3.test_s3"
    assert instance.query(query3) == "1\t1\n"

    metrics3 = get_s3_events(instance)
    minio3 = get_minio_stat(cluster)

    assert (
        metrics3["S3ReadRequestsCount"] - metrics2["S3ReadRequestsCount"]
        == minio3["get_requests"] - minio2["get_requests"]
    )
    assert (
        metrics3["S3WriteRequestsCount"] - metrics2["S3WriteRequestsCount"]
        == minio3["set_requests"] - minio2["set_requests"]
    )
    stat3 = get_query_stat(instance, query3)

    # With async reads profile events are not updated fully because reads are done in a separate thread.
    # for metric in stat3:
    #    print(metric)
    #    assert stat3[metric] == metrics3[metric] - metrics2[metric]
