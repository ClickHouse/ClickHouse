#!/usr/bin/env python3

import logging
import os
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


# Runs custom python-based S3 endpoint.
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
            main_configs=[
                "configs/storage_conf.xml",
            ],
            user_configs=[
                "configs/upload_min_size.xml",
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


def test_dataloss(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_failover_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    # Must throw an exception because we use proxy which always fail
    # CompleteMultipartUpload requests
    with pytest.raises(Exception):
        node.query("INSERT INTO s3_failover_test VALUES (1, 'Hello')")
