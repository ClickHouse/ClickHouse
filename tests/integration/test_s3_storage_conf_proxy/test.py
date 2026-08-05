import logging
import os
import time

import pytest

import helpers.s3_url_proxy_tests_util as proxy_util
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node", main_configs=["configs/config.d/storage_conf.xml"], with_minio=True
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        proxy_util.run_resolver(cluster, os.path.dirname(__file__))
        logging.info("Proxy resolver started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("policy", ["s3", "s3_with_resolver"])
def test_s3_with_proxy_list(cluster, policy):
    proxy_util.simple_storage_test(
        cluster, cluster.instances["node"], ["proxy1", "proxy2"], policy
    )
