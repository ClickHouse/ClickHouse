import logging
import os
import time

import pytest
from helpers.cluster import ClickHouseCluster


# Runs simple proxy resolver in python env container.
def run_resolver(cluster):
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "proxy-resolver", "resolver.py"),
        "resolver.py",
    )
    cluster.exec_in_container(container_id, ["python", "resolver.py"], detach=True)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "remote_proxy_node",
            main_configs=["configs/config.d/proxy_remote.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_resolver(cluster)
        logging.info("Proxy resolver started")

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
            time.sleep(i)
        else:
            assert False, "http method not found in logs"


def test_s3_with_remote_resolver(cluster):
    node = cluster.instances["remote_proxy_node"]

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

    check_proxy_logs(cluster, "proxy1")
