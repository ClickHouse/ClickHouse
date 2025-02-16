import logging
import time

import pytest
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


def check_proxy_logs(cluster, proxy_instance, http_methods={"POST", "PUT", "GET"}):
    for i in range(10):
        logs = cluster.get_container_logs(proxy_instance)
        # Check with retry that all possible interactions with Minio are present
        for http_method in http_methods:
            if logs.find(http_method + " http://minio1") >= 0:
                return
            time.sleep(1)
        else:
            assert False, f"{http_methods} method not found in logs of {proxy_instance}"


@pytest.mark.parametrize("policy", ["s3"])
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

    for proxy in ["proxy1", "proxy2"]:
        check_proxy_logs(cluster, proxy, ["PUT", "GET"])
