import ssl
import pytest
import os.path as p
import os
from helpers.cluster import ClickHouseCluster
from helpers.client import Client
import urllib.request, urllib.parse

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("server", base_config_dir="configs", main_configs=["configs/server.crt", "configs/server.key"])


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def execute_query_https(host, port, query):
    url = (
        f"https://{host}:{port}/?query={urllib.parse.quote(query)}"
    )

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request, context=ctx).read()
    return response.decode("utf-8")


def execute_query_http(host, port, query):
    url = (
        f"http://{host}:{port}/?query={urllib.parse.quote(query)}"
    )

    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request).read()
    return response.decode("utf-8")


def test_connections():

    client = Client(server.ip_address, 9000, command=cluster.client_bin_path)
    assert client.query("SELECT 1") == "1\n"

    client = Client(server.ip_address, 9440, command=cluster.client_bin_path, secure=True, config=f"{SCRIPT_DIR}/configs/client.xml")
    assert client.query("SELECT 1") == "1\n"

    client = Client(server.ip_address, 9001, command=cluster.client_bin_path)
    assert client.query("SELECT 1") == "1\n"

    assert execute_query_http(server.ip_address, 8123, "SELECT 1") == "1\n"

    assert execute_query_https(server.ip_address, 8443, "SELECT 1") == "1\n"

    assert execute_query_https(server.ip_address, 8444, "SELECT 1") == "1\n"
