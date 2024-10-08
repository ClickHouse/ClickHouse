import logging

import pytest

from helpers.cluster import ClickHouseCluster


def check_proxy_logs(cluster, proxy_instance):
    logs = cluster.get_container_logs(proxy_instance)
    # Check that all possible interactions with Minio are present
    for http_method in ["PUT", "GET", "POST"]:
        assert logs.find(http_method + " https://minio1") >= 0


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/ssl.xml",
            ],
            with_minio=True,
            minio_certs_dir="minio_certs",
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("policy", ["s3_secure", "s3_secure_with_proxy"])
def test_s3_with_https(cluster, policy):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            policy
        )
    )

    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    assert (
        node.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    node.query("DROP TABLE IF EXISTS s3_test SYNC")

    if policy.find("proxy") != -1:
        check_proxy_logs(cluster, "proxy1")
