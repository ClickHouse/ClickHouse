#!/usr/bin/env python3
# Regression test for the AWS-SDK 301 redirect SSRF: a malicious/compromised
# S3-compatible endpoint returns 301 Moved Permanently with a Location header (or
# <Endpoint> XML) pointing at an arbitrary host. Client::getURIFromError must validate
# that target against RemoteHostFilter (like the Poco 307 path already does) instead of
# blindly following it. Without the fix the s3() query reaches the disallowed host
# (SSRF to internal services / cloud metadata); with the fix it fails with UNACCEPTABLE_URL.
import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster


def run_endpoint(cluster):
    logging.info("Starting custom S3 endpoint")
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "s3_endpoint", "endpoint.py"),
        "endpoint.py",
    )
    cluster.exec_in_container(container_id, ["python", "endpoint.py"], detach=True)

    # Wait for S3 endpoint start
    num_attempts = 100
    for attempt in range(num_attempts):
        ping_response = cluster.exec_in_container(
            cluster.get_container_id("resolver"),
            ["curl", "-s", "http://resolver:8080/"],
            nothrow=True,
        )
        if ping_response != "OK":
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            else:
                time.sleep(1)
        else:
            break

    logging.info("S3 endpoint started")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/config.xml"],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_endpoint(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def _followed(cluster):
    return cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", "http://resolver:8080/followed"],
        nothrow=True,
    )


def test_301_redirect_target_is_host_filtered(cluster):
    node = cluster.instances["node"]

    # The s3() endpoint (resolver:8080) is allow-listed, but it 301-redirects to its own
    # raw IP, which is NOT allow-listed. The redirect target must be rejected.
    error = node.query_and_get_error(
        "SELECT * FROM s3('http://resolver:8080/bucket/key', 'TSV', 'x String') "
        "SETTINGS s3_max_redirects=5"
    )
    assert "not allowed in configuration file" in error, (
        "expected RemoteHostFilter to reject the 301 redirect target, got: " + error
    )

    # And the server must not have sent a single request to the disallowed target.
    assert _followed(cluster) == "NO", "ClickHouse followed the 301 to a disallowed host (SSRF)"
