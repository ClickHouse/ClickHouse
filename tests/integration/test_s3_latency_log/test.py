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
CONNECT_TIMEOUT_MICROSECONDS = 500000


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

    # We cannot properly check connect timeouts because we will
    # have almost random amount of them, because of usage of bottle server
    # as "slow mock S3 endpoint". So we just check that we have some connect
    # times.
    for i, (bucket, cnt) in enumerate(connect_times_after):
        cnt_before = connect_times_before[i][1]

        # We only have more reconnects, number doesn't decrease.
        assert cnt >= cnt_before
        if i > 0:
            assert cnt >= connect_times_after[i - 1][1]

        if i >= 1:
            # We should have at least some connects >= 10 millisecond
            assert cnt > 0


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
