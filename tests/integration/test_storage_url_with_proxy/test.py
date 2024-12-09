import base64
import hashlib
import hmac
import logging
import time
from datetime import datetime

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


def check_proxy_logs(cluster, proxy_instance, http_methods):
    minio_ip = cluster.get_instance_ip("minio1")
    for i in range(10):
        logs = cluster.get_container_logs(proxy_instance)
        # Check with retry that all possible interactions with Minio are present
        for http_method in http_methods:
            method_with_domain = http_method + " http://minio1"
            method_with_ip = http_method + f" http://{minio_ip}"

            logging.info(f"Method with ip: {method_with_ip}")

            has_get_minio_logs = (
                logs.find(method_with_domain) >= 0 or logs.find(method_with_ip) >= 0
            )
            if has_get_minio_logs:
                return
            time.sleep(1)
        else:
            assert False, "http method not found in logs"


def test_s3_with_proxy_list(cluster):
    node = cluster.instances["proxy_list_node"]

    # insert into function url uses POST and minio expects PUT
    node.query(
        """
        INSERT INTO FUNCTION
        s3('http://minio1:9001/root/data/ch-proxy-test/test.csv', 'minio', 'minio123', 'CSV', 'key String, value String')
        VALUES ('color','red'),('size','10')
        """
    )

    content_type = "application/zstd"
    date = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
    resource = "/root/data/ch-proxy-test/test.csv"
    get_sig_string = f"GET\n\n{content_type}\n{date}\n{resource}"
    password = "minio123"

    get_digest = hmac.new(
        password.encode("utf-8"), get_sig_string.encode("utf-8"), hashlib.sha1
    ).digest()
    get_signature = base64.b64encode(get_digest).decode("utf-8")
    assert (
        node.query(
            "SELECT * FROM url('http://minio1:9001/root/data/ch-proxy-test/test.csv', 'CSV', 'a String, b String',"
            f"headers('Host'='minio1', 'Date'= '{date}', 'Content-Type'='{content_type}',"
            f"'Authorization'='AWS minio:{get_signature}')) FORMAT Values"
        )
        == "('color','red'),('size','10')"
    )

    check_proxy_logs(cluster, "proxy1", ["GET"])
