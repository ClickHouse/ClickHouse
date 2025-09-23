import logging
import os

import pytest

import helpers.s3_url_proxy_tests_util as proxy_util
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        # minio_certs_dir is set only once and used by all instances

        cluster.add_instance(
            "remote_proxy_node",
            main_configs=[
                "configs/config.d/proxy_remote.xml",
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
            minio_certs_dir="minio_certs",
        )

        cluster.add_instance(
            "remote_proxy_node_no_proxy",
            main_configs=[
                "configs/config.d/proxy_remote_no_proxy.xml",
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
        )

        cluster.add_instance(
            "proxy_list_node",
            main_configs=[
                "configs/config.d/proxy_list.xml",
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
        )

        cluster.add_instance(
            "proxy_list_node_no_proxy",
            main_configs=[
                "configs/config.d/proxy_list_no_proxy.xml",
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
        )

        cluster.add_instance(
            "env_node",
            main_configs=[
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
            env_variables={
                "https_proxy": "https://proxy1",
            },
            instance_env_variables=True,
        )

        cluster.add_instance(
            "env_node_no_proxy",
            main_configs=[
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
            env_variables={
                "https_proxy": "https://proxy1",
                "no_proxy": "not_important_host,,  minio1  ,",
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


def test_s3_with_https_proxy_list_no_proxy(cluster):
    proxy_util.simple_test_assert_no_proxy(
        cluster, ["proxy1", "proxy2"], "https", "proxy_list_node_no_proxy"
    )


def test_s3_with_https_env_no_proxy(cluster):
    proxy_util.simple_test_assert_no_proxy(
        cluster, ["proxy1"], "https", "env_node_no_proxy"
    )


def test_s3_with_https_remote_no_proxy(cluster):
    proxy_util.simple_test_assert_no_proxy(
        cluster, ["proxy1"], "https", "remote_proxy_node_no_proxy"
    )


def test_s3_with_https_proxy_list(cluster):
    proxy_util.simple_test(cluster, ["proxy1", "proxy2"], "https", "proxy_list_node")


def test_s3_with_https_remote_proxy(cluster):
    proxy_util.simple_test(cluster, ["proxy1", "proxy2"], "https", "remote_proxy_node")


def test_s3_with_https_env_proxy(cluster):
    proxy_util.simple_test(cluster, ["proxy1"], "https", "env_node")
