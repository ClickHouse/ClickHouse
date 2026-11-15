import logging
import os

import pytest

import helpers.s3_url_proxy_tests_util as proxy_util
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "remote_proxy_node",
            main_configs=[
                "configs/config.d/proxy_remote.xml",
            ],
            with_minio=True,
        )

        cluster.add_instance(
            "proxy_list_node",
            main_configs=[
                "configs/config.d/proxy_list.xml",
            ],
            with_minio=True,
        )

        cluster.add_instance(
            "env_node",
            main_configs=[
                "configs/config.d/proxy_env.xml",
            ],
            with_minio=True,
            env_variables={
                "https_proxy": "http://proxy1",
            },
            instance_env_variables=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        proxy_util.run_resolver(cluster, os.path.dirname(__file__))
        logging.info("Proxy resolver started")

        yield cluster
    finally:
        cluster.shutdown()


def test_s3_with_https_proxy_list(cluster):
    proxy_util.simple_test(cluster, ["proxy1", "proxy2"], "https", "proxy_list_node")


def test_s3_with_https_remote_proxy(cluster):
    proxy_util.simple_test(cluster, ["proxy1", "proxy2"], "https", "remote_proxy_node")


def test_s3_with_https_env_proxy(cluster):
    proxy_util.simple_test(cluster, ["proxy1"], "https", "env_node")
