import os
import os.path as p
import socket
import ssl
import subprocess
import urllib.parse
import urllib.request
import warnings

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.proxy1 import Proxy1

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance(
    "server",
    base_config_dir="configs",
    main_configs=["configs/server.crt", "configs/server.key"],
)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def execute_query_https(host, port, query, version=None):
    url = f"https://{host}:{port}/?query={urllib.parse.quote(query)}"

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    if version:
        ctx.minimum_version = version
        ctx.maximum_version = version

    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request, context=ctx).read()
    return response.decode("utf-8")


def execute_query_https_unsupported(host, port, query, version=None):
    try:
        execute_query_https(host, port, query, version)
    except Exception as e:
        e_text = str(e)
        if "NO_PROTOCOLS_AVAILABLE" in e_text:
            return True
        if "TLSV1_ALERT_PROTOCOL_VERSION" in e_text:
            return True
        raise
    return False


def execute_query_http(host, port, query):
    url = f"http://{host}:{port}/?query={urllib.parse.quote(query)}"

    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request).read()
    return response.decode("utf-8")


def netcat(hostname, port, content):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, port))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    data = []
    while 1:
        d = s.recv(1024)
        if len(d) == 0:
            break
        data.append(d)
    s.close()
    return b"".join(data)


def test_connections():
    client = Client(server.ip_address, 9000, command=cluster.client_bin_path)
    assert client.query("SELECT 1") == "1\n"

    client = Client(
        server.ip_address,
        9440,
        command=cluster.client_bin_path,
        secure=True,
        config=f"{SCRIPT_DIR}/configs/client.xml",
    )
    assert client.query("SELECT 1") == "1\n"

    client = Client(server.ip_address, 9001, command=cluster.client_bin_path)
    assert client.query("SELECT 1") == "1\n"

    assert execute_query_http(server.ip_address, 8123, "SELECT 1") == "1\n"

    assert execute_query_https(server.ip_address, 8443, "SELECT 1") == "1\n"

    assert execute_query_https(server.ip_address, 8444, "SELECT 1") == "1\n"

    warnings.filterwarnings("ignore", category=DeprecationWarning)

    assert execute_query_https_unsupported(
        server.ip_address, 8445, "SELECT 1", version=ssl.TLSVersion.SSLv3
    )
    assert execute_query_https_unsupported(
        server.ip_address, 8445, "SELECT 1", version=ssl.TLSVersion.TLSv1
    )
    assert execute_query_https_unsupported(
        server.ip_address, 8445, "SELECT 1", version=ssl.TLSVersion.TLSv1_1
    )
    assert (
        execute_query_https(
            server.ip_address, 8445, "SELECT 1", version=ssl.TLSVersion.TLSv1_2
        )
        == "1\n"
    )
    assert (
        execute_query_https(
            server.ip_address, 8445, "SELECT 1", version=ssl.TLSVersion.TLSv1_3
        )
        == "1\n"
    )

    assert execute_query_https_unsupported(
        server.ip_address, 8446, "SELECT 1", version=ssl.TLSVersion.SSLv3
    )
    assert execute_query_https_unsupported(
        server.ip_address, 8446, "SELECT 1", version=ssl.TLSVersion.TLSv1
    )
    assert execute_query_https_unsupported(
        server.ip_address, 8446, "SELECT 1", version=ssl.TLSVersion.TLSv1_1
    )
    assert execute_query_https_unsupported(
        server.ip_address, 8446, "SELECT 1", version=ssl.TLSVersion.TLSv1_2
    )
    assert (
        execute_query_https(
            server.ip_address, 8446, "SELECT 1", version=ssl.TLSVersion.TLSv1_3
        )
        == "1\n"
    )


# tests when using PROXYv1 with enabled auth_use_forwarded_address that forwarded address is used for authentication and query's source address
def test_proxy_1():

    # default user
    proxy = Proxy1("TCP4 123.231.132.213 255.255.255.255 12345 65535")
    proxy_client = Client(
        "localhost",
        proxy.start((server.ip_address, 9100)),
        command=cluster.client_bin_path,
    )
    query_id = proxy_client.query("SELECT currentQueryID()")[:-1]
    cluster.instances["server"].query("SYSTEM FLUSH LOGS")
    client = Client(server.ip_address, 9000, command=cluster.client_bin_path)
    assert (
        client.query(
            f"SELECT forwarded_for, address, port, initial_address, initial_port FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryStart'"
        )
        == "123.231.132.213:12345\t::ffff:123.231.132.213\t12345\t::ffff:123.231.132.213\t12345\n"
    )

    # user123 only allowed from 123.123.123.123
    proxy = Proxy1("TCP4 123.123.123.123 255.255.255.255 12345 65535")
    proxy_client = Client(
        "localhost",
        proxy.start((server.ip_address, 9100)),
        command=cluster.client_bin_path,
    )
    query_id = proxy_client.query("SELECT currentQueryID()", user="user123")[:-1]
    cluster.instances["server"].query("SYSTEM FLUSH LOGS")
    client = Client(server.ip_address, 9000, command=cluster.client_bin_path)
    assert (
        client.query(
            f"SELECT forwarded_for, address, port, initial_address, initial_port FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryStart'"
        )
        == "123.123.123.123:12345\t::ffff:123.123.123.123\t12345\t::ffff:123.123.123.123\t12345\n"
    )

    # user123 is not allowed from other than 123.123.123.123
    proxy = Proxy1("TCP4 127.0.0.1 255.255.255.255 12345 65535")
    proxy_client = Client(
        "localhost",
        proxy.start((server.ip_address, 9100)),
        command=cluster.client_bin_path,
    )
    try:
        proxy_client.query("SELECT currentQueryID()", user="user123")
    except Exception as e:
        assert str(e).find("Exception: user123: Authentication failed") >= 0
    else:
        assert False, "Expected 'Exception: user123: Authentication failed'"


# tests PROXYv1 over HTTP
def test_http_proxy_1():
    proxy = Proxy1()
    port = proxy.start((server.ip_address, 8223))

    assert execute_query_http("localhost", port, "SELECT 1") == "1\n"
