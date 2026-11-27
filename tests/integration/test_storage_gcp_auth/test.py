import json
import logging
import os
import time

import pytest
import requests

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_gcs_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


TOKEN_PATH = "computeMetadata/v1/instance/service-accounts"
SERVICE_ACCOUNT = "my-account"


def run_gcs_mocks(cluster):
    logging.info("Starting gcs mocks")
    mocks = (
        ("echo.py", "resolver", ["22234"]),
        ("auth.py", "resolver", ["80", TOKEN_PATH, SERVICE_ACCOUNT]),
    )

    for mock_filename, container, args in mocks:
        container_id = cluster.get_container_id(container)
        current_dir = os.path.dirname(__file__)
        cluster.copy_file_to_container(
            container_id,
            os.path.join(current_dir, "gcs_mocks", mock_filename),
            mock_filename,
        )

        cluster.exec_in_container(
            container_id, ["python", mock_filename] + args, detach=True
        )

    # Wait for S3 mocks to start
    for mock_filename, container, args in mocks:
        port = args[0]
        num_attempts = 100
        for attempt in range(num_attempts):
            ping_response = cluster.exec_in_container(
                cluster.get_container_id(container),
                ["curl", "-s", f"http://localhost:{port}/ping"],
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
                logging.debug(
                    f"mock {mock_filename} ({port}) answered {ping_response} on attempt {attempt}"
                )
                break

    logging.info("S3 mocks started")


def test_gcp_auth(started_cluster):
    node = started_cluster.instances["node"]

    # Reset mock counters so the test is repeatable
    resolver_id = started_cluster.get_container_id("resolver")
    for port in [80, 22234]:
        started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", f"http://localhost:{port}/reset"],
            nothrow=True,
        )

    def get_num_requests():
        count_response = started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", f"http://localhost/counter"],
            nothrow=True,
        )

        return int(count_response)

    node.query("DROP TABLE IF EXISTS s3_table")
    node.query(
        "CREATE TABLE s3_table (line String) ENGINE = S3(gcs_conn, filename='test.txt', format='LineAsString')"
    )

    assert get_num_requests() == 0
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    # Wait to refresh token
    time.sleep(4)

    assert get_num_requests() == 2
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    assert get_num_requests() == 4
    assert (
        node.query(
            "SELECT * FROM s3(gcs_conn, filename='test.txt', format='LineAsString')"
        )
        == "OK\n"
    )

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_conn_bad, filename='test.txt', format='LineAsString')"
        )

    assert "AUTHENTICATION_FAILED" in ei.value.stderr
