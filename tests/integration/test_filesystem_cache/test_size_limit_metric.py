import logging
import os
import random
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock
from helpers.utility import SafeThread, generate_values, replace_config

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_test_size_limit_metric",
            main_configs=[
                "config.d/filesystem_caches_path.xml",
                "config.d/filesystem_caches.xml",
            ],
            user_configs=[
                "users.d/cache_on_write_operations.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_cache_size_limit_metric(cluster):
    node = cluster.instances["node_test_size_limit_metric"]
    assert 1234 == int(
        node.query(
            "SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSizeLimit'"
        )
    )
