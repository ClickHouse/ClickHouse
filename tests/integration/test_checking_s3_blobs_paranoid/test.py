#!/usr/bin/env python3

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock


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
                "configs/setting.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8083")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


def test_upload_after_check_works(cluster, broken_s3):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_upload_after_check_works (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    broken_s3.setup_fake_upload(1)

    error = node.query_and_get_error(
        "INSERT INTO s3_upload_after_check_works VALUES (1, 'Hello')"
    )

    assert "Code: 499" in error, error
    assert "Immediately after upload" in error, error
    assert "suddenly disappeared" in error, error
