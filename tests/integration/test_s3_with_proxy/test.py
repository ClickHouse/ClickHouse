import logging
import os

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
    minio_client = cluster.minio_client

    if minio_client.bucket_exists(cluster.minio_bucket):
        minio_client.remove_bucket(cluster.minio_bucket)

    minio_client.make_bucket(cluster.minio_bucket)


# Runs simple proxy resolver in python env container.
def run_resolver(cluster):
    container_id = cluster.get_container_id('resolver')
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "proxy-resolver", "resolver.py"),
                                   "resolver.py")
    cluster.copy_file_to_container(container_id, os.path.join(current_dir, "proxy-resolver", "entrypoint.sh"),
                                   "entrypoint.sh")
    cluster.exec_in_container(container_id, ["/bin/bash", "entrypoint.sh"], detach=True)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", config_dir="configs", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        run_resolver(cluster)
        logging.info("Proxy resolver started")

        yield cluster
    finally:
        cluster.shutdown()


def check_proxy_logs(cluster, proxy_instance):
    logs = cluster.get_container_logs(proxy_instance)
    # Check that all possible interactions with Minio are present
    for http_method in ["POST", "PUT", "GET", "DELETE"]:
        assert logs.find(http_method + " http://minio1") >= 0


@pytest.mark.parametrize(
    "policy", ["s3", "s3_with_resolver"]
)
def test_s3_with_proxy_list(cluster, policy):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """
        .format(policy)
    )

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    assert node.query("SELECT * FROM s3_test order by id FORMAT Values") == "(0,'data'),(1,'data')"

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")

    for proxy in ["proxy1", "proxy2"]:
        check_proxy_logs(cluster, proxy)
