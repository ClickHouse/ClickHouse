#!/usr/bin/env python3
# Regression test for the AWS-SDK 301 redirect SSRF: a malicious/compromised
# S3-compatible endpoint returns 301 Moved Permanently with a Location header (or
# <Endpoint> XML) pointing at an arbitrary host. Client::doRequest must validate
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

        cluster.instances["node"].append_hosts(
            "redirected", cluster.get_instance_ip("resolver")
        )
        cluster.instances["node"].append_hosts(
            "unreachable", cluster.get_instance_ip("resolver")
        )
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


def _initial_requests(cluster, bucket):
    return cluster.exec_in_container(
        cluster.get_container_id("resolver"),
        ["curl", "-s", f"http://resolver:8080/initial_requests/{bucket}"],
        nothrow=True,
    )


@pytest.mark.parametrize("bucket", ["bucket", "virtual"])
def test_301_redirect_target_is_host_filtered(cluster, bucket):
    node = cluster.instances["node"]

    # The endpoint is allow-listed, but its redirect target is not. For the virtual-hosted
    # case, only the normalized service endpoint is allow-listed, not the bucket host.
    # NOSIGN keeps the request anonymous so it reaches the redirect rather than being refused by the
    # server-managed S3 credential restriction (this test is about the host filter, not credentials).
    error = node.query_and_get_error(
        f"SELECT * FROM s3('http://resolver:8080/{bucket}/key', NOSIGN, 'TSV', 'x String') "
        "SETTINGS s3_max_redirects=5"
    )
    assert "not allowed in configuration file" in error, (
        "expected RemoteHostFilter to reject the 301 redirect target, got: " + error
    )

    # And the server must not have sent a single request to the disallowed target.
    assert _followed(cluster) == "NO", "ClickHouse followed the 301 to a disallowed host (SSRF)"


def test_failed_301_redirect_is_not_cached(cluster):
    node = cluster.instances["node"]
    table = "s3_redirect_cache"
    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"CREATE TABLE {table} (x UInt8) "
        "ENGINE = S3('http://resolver:8080/cache/key.csv', NOSIGN, 'CSV')"
    )
    try:
        for _ in range(2):
            error = node.query_and_get_error(
                f"INSERT INTO {table} SELECT 1 SETTINGS s3_truncate_on_insert=1"
            )
            assert "AccessDenied" in error
        assert _initial_requests(cluster, "cache") == "2"
    finally:
        node.query(f"DROP TABLE {table}")


def test_head_redirect_is_cached_after_list_access_denied(cluster):
    node = cluster.instances["node"]
    table = "s3_head_redirect_cache"
    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"CREATE TABLE {table} (x UInt8) "
        "ENGINE = S3('http://resolver:8080/head/key.csv', NOSIGN, 'CSV')"
    )
    try:
        error = node.query_and_get_error(f"SELECT * FROM {table}")
        assert "AccessDenied" in error
        assert node.query(f"SELECT * FROM {table}") == "1\n"
    finally:
        node.query(f"DROP TABLE {table}")


def test_head_redirect_is_not_cached_after_network_error(cluster):
    node = cluster.instances["node"]
    table = "s3_head_redirect_network_error"
    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"CREATE TABLE {table} (x UInt8) "
        "ENGINE = S3('http://resolver:8080/network/key.csv', NOSIGN, 'CSV')"
    )
    try:
        for _ in range(2):
            assert node.query_and_get_error(f"SELECT * FROM {table}")
        assert _initial_requests(cluster, "network") == "4"
    finally:
        node.query(f"DROP TABLE {table}")
