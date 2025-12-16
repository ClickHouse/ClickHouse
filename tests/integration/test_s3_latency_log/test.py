#!/usr/bin/env python3

import logging
import os
import time
from ast import literal_eval

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

RESOLVER_CONTAINER_NAME = "resolver"
RESOLVER_PORT = 8080
CONNECT_TIMEOUT_MICROSECONDS = 1000000


# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting custom S3 endpoint")
    script_dir = os.path.join(os.path.dirname(__file__), "s3_endpoint")
    start_mock_servers(
        cluster, script_dir, [("endpoint.py", RESOLVER_CONTAINER_NAME, RESOLVER_PORT)]
    )
    logging.info("S3 endpoint started")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/storage_conf.xml",
            ],
            user_configs=[
                "configs/s3_conf.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_endpoint(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_latency_log(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_latency_log_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3'
        """
    )

    node.query("SYSTEM FLUSH LOGS")
    latency_log_before = literal_eval(
        node.query(
            """
        SELECT (LatencyEvent_S3ConnectMicroseconds, LatencyEvent_S3FirstByteWriteAttempt1Microseconds, LatencyEvent_S3FirstByteWriteAttempt2Microseconds, LatencyEvent_S3FirstByteWriteAttemptNMicroseconds)
        FROM system.latency_log
        ORDER BY event_time_microseconds DESC LIMIT 1
        """
        )
    )
    connect_times_before = literal_eval(
        node.query(
            """
        WITH (SELECT LatencyEvent_DiskS3ConnectMicroseconds FROM system.latency_buckets) AS buckets
        SELECT format('[{}]', arrayStringConcat(arrayMap((t, c) ->
            format('({},{})', toString(t), c), buckets, LatencyEvent_DiskS3ConnectMicroseconds), ','))
        FROM system.latency_log
        ORDER BY event_time_microseconds DESC LIMIT 1
        """
        )
    )
    resolver_requests_cnt = int(
        cluster.exec_in_container(
            cluster.get_container_id("resolver"),
            ["curl", "-s", f"http://{RESOLVER_CONTAINER_NAME}:{RESOLVER_PORT}/total"],
            nothrow=True,
        )
    )
    connect_timed_out = int(
        node.count_in_log(
            f"Failed to make request to.*{RESOLVER_CONTAINER_NAME}:{RESOLVER_PORT}.*Timeout: connect timed out"
        )
    )
    logging.debug("Latency log before - %s", latency_log_before)
    logging.debug("Connect times before - %s", connect_times_before)
    logging.debug("Connection timed out requests count before - %d", connect_timed_out)
    logging.debug("Resolver requests count before - %d", resolver_requests_cnt)

    node.query("INSERT INTO s3_latency_log_test VALUES (1, 'Hello')")

    node.query("SYSTEM FLUSH LOGS")
    latency_log_after = literal_eval(
        node.query(
            """
        SELECT (LatencyEvent_S3ConnectMicroseconds, LatencyEvent_S3FirstByteWriteAttempt1Microseconds, LatencyEvent_S3FirstByteWriteAttempt2Microseconds, LatencyEvent_S3FirstByteWriteAttemptNMicroseconds)
        FROM system.latency_log
        ORDER BY event_time_microseconds DESC LIMIT 1
        """
        )
    )
    connect_times_after = literal_eval(
        node.query(
            """
        WITH (SELECT LatencyEvent_DiskS3ConnectMicroseconds FROM system.latency_buckets) AS buckets
        SELECT format('[{}]', arrayStringConcat(arrayMap((t, c) ->
            format('({},{})', toString(t), c), buckets, LatencyEvent_DiskS3ConnectMicroseconds), ','))
        FROM system.latency_log
        ORDER BY event_time_microseconds DESC LIMIT 1
        """
        )
    )
    connect_timed_out = (
        int(
            node.count_in_log(
                f"Failed to make request to.*{RESOLVER_CONTAINER_NAME}:{RESOLVER_PORT}.*Timeout: connect timed out"
            )
        )
        - connect_timed_out
    )
    resolver_requests_cnt = (
        int(
            cluster.exec_in_container(
                cluster.get_container_id("resolver"),
                [
                    "curl",
                    "-s",
                    f"http://{RESOLVER_CONTAINER_NAME}:{RESOLVER_PORT}/total",
                ],
                nothrow=True,
            )
        )
        - resolver_requests_cnt
    )
    logging.debug("Latency log after - %s", latency_log_after)
    logging.debug("Connect times after - %s", connect_times_after)
    logging.debug("Connection timed out requests count after - %d", connect_timed_out)
    logging.debug("Resolver requests count after - %d", resolver_requests_cnt)

    for idx_event, event in enumerate(latency_log_after):
        prev = 0

        for i, cnt in enumerate(event):
            curr_cnt = cnt - latency_log_before[idx_event][i]
            assert (
                curr_cnt >= prev
            ), "All counters in latency log should be in non-decreasing order"
            prev = curr_cnt

    below_second_connect_time_cnt = 0
    other_connect_time_cnt = 0

    # Count connect times that are below connect timeout, and then we check that
    # for requests with connection timeout error we have correct latency log entries.
    # Also we expect no connection timeout for requests to minio.
    for i, (bucket, cnt) in enumerate(connect_times_after):
        cnt_before = connect_times_before[i][1]

        if bucket < CONNECT_TIMEOUT_MICROSECONDS:
            below_second_connect_time_cnt = cnt - cnt_before
        else:
            other_connect_time_cnt = cnt - cnt_before
            break

    logging.debug(
        "Below second connect time cnt - %d, other connect time cnt - %d",
        below_second_connect_time_cnt,
        other_connect_time_cnt,
    )
    assert (
        below_second_connect_time_cnt + connect_timed_out == other_connect_time_cnt
    ), "Number of connection timed out requests does not match the number of such requests in the latency log"

    # Check that the number of requests to resolver is not greater than the number of such requests in the latency log.
    # We can't check for equality, because there are requests coming to minio, and we don't know the exact number of connections
    # to minio, as such connections can be reused.
    assert (
        resolver_requests_cnt + connect_timed_out <= connect_times_after[-1][1]
    ), "Number of requests to resolver should not be greater than the number of max connect time requests in the latency log"

    # We won't record latency for the first byte if there was a connection error, so it is possible to have no requests in the first byte write
    # latency log for the second attempt, but there definitely should be something for second+ attempt.
    if connect_timed_out > 0:
        assert (
            latency_log_after[2][0]
            + latency_log_after[3][0]
            - latency_log_before[2][0]
            - latency_log_before[3][0]
            > 0
        ), "There should be second+ attempt requests in the latency log if there were some connect timed out errors"

    node.query("DROP TABLE s3_latency_log_test")
