import logging
import time

import pytest

import helpers.s3_url_proxy_tests_util as proxy_util
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/proxy_list.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("policy", ["s3"])
def test_s3_with_proxy_list(cluster, policy):
    proxy_util.simple_storage_test(
        cluster, cluster.instances["node"], ["proxy1"], policy
    )
