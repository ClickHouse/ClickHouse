import http.client

import pytest
from helpers.cluster import ClickHouseCluster
import urllib.request, urllib.parse
import ssl
import os.path
import time

HTTPS_PORT = 8443
# It's important for the node to work at this IP because 'server-cert.pem' requires that (see server-ext.cnf).
NODE_IP = "10.5.172.77"
NODE_IP_WITH_HTTPS_PORT = NODE_IP + ":" + str(HTTPS_PORT)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    ipv4_address=NODE_IP,
    main_configs=[
        "configs/ssl_config.xml",
        "certs/server-key.pem",
        "certs/server-cert.pem",
        "certs/ca-cert.pem",
    ],
    user_configs=["configs/users_with_ssl_auth.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_ssl_context(cert_name):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=f"{SCRIPT_DIR}/certs/ca-cert.pem")
    if cert_name:
        context.load_cert_chain(
            f"{SCRIPT_DIR}/certs/{cert_name}-cert.pem",
            f"{SCRIPT_DIR}/certs/{cert_name}-key.pem",
        )
        context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    return context


def execute_query_https(
    query, user, enable_ssl_auth=True, cert_name=None, password=None
):
    headers = {"X-ClickHouse-User": user}
    if enable_ssl_auth:
        headers["X-ClickHouse-SSL-Certificate-Auth"] = "on"

    if password:
        headers["X-ClickHouse-Key"] = password

    headers["Host"] = "clickhouse.com"

    url = f"{NODE_IP_WITH_HTTPS_PORT}"
    # source_address should be a bindable address - just binding to localhost and a random port
    c = http.client.HTTPSConnection(
        host=url, source_address=("10.5.1.1", 12345), context=get_ssl_context(cert_name)
    )
    path = f"/?query={urllib.parse.quote(query)}"
    c.request("POST", path, headers=headers)
    response = c.getresponse().read()
    return response.decode("utf-8")


def test_tls_sni():
    execute_query_https(
        "SELECT 1 settings log_queries=1;", user="john", cert_name="client1"
    )
    execute_query_https("SYSTEM FLUSH LOGS;", user="john", cert_name="client1")
    time.sleep(1)
    res = execute_query_https(
        "SELECT tls_sni from system.query_log LIMIT 1;",
        user="john",
        cert_name="client1",
    )
    assert res == "10.5.1.1:12345\n"
    # also just check the http_host field too
    res = execute_query_https(
        "SELECT http_host from system.query_log LIMIT 1;",
        user="john",
        cert_name="client1",
    )
    assert res == "clickhouse.com\n"
