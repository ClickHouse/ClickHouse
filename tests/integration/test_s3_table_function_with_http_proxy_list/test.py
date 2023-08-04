import logging
import time

import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "proxy_list_node",
            main_configs=["configs/config.d/proxy_list.xml"],
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
            assert False, "http method not found in logs"


def test_s3_with_proxy_list(cluster):
    node = cluster.instances["proxy_list_node"]

    node.query(
        """
        INSERT INTO FUNCTION
        s3('http://minio1:9001/root/data/ch-proxy-test/test.csv', 'minio', 'minio123', 'CSV', 'key String, value String')
        VALUES ('color','red'),('size','10')
        """
    )

    assert (
        node.query(
            "SELECT * FROM s3('http://minio1:9001/root/data/ch-proxy-test/test.csv', 'minio', 'minio123', 'CSV') FORMAT Values"
        )
        == "('color','red'),('size','10')"
    )

    assert (
        node.query(
            "SELECT * FROM s3('http://minio1:9001/root/data/ch-proxy-test/test.csv', 'minio', 'minio123', 'CSV') FORMAT Values"
        )
        == "('color','red'),('size','10')"
    )

    for proxy in ["proxy1", "proxy2"]:
        check_proxy_logs(cluster, proxy)
