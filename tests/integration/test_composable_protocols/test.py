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

    data = "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\021ClickHouse client\024\r\253\251\003\0\007default\0\004\001\0\001\0\0\t0.0.0.0:0\001\tmilovidov\021milovidov-desktop\21ClickHouse client\024\r\253\251\003\0\001\0\0\0\002\001\025SELECT 'Hello, world'\002\0\247\203\254l\325\\z|\265\254F\275\333\206\342\024\202\024\0\0\0\n\0\0\0\240\01\0\02\377\377\377\377\0\0\0"
    assert (
        netcat(server.ip_address, 9100, bytearray(data, "latin-1")).find(
            bytearray("Hello, world", "latin-1")
        )
        >= 0
    )

    data_user_allowed = "PROXY TCP4 123.123.123.123 255.255.255.255 65535 65535\r\n\0\021ClickHouse client\024\r\253\251\003\0\007user123\0\004\001\0\001\0\0\t0.0.0.0:0\001\tmilovidov\021milovidov-desktop\21ClickHouse client\024\r\253\251\003\0\001\0\0\0\002\001\025SELECT 'Hello, world'\002\0\247\203\254l\325\\z|\265\254F\275\333\206\342\024\202\024\0\0\0\n\0\0\0\240\01\0\02\377\377\377\377\0\0\0"
    assert (
        netcat(server.ip_address, 9100, bytearray(data_user_allowed, "latin-1")).find(
            bytearray("Hello, world", "latin-1")
        )
        >= 0
    )

    data_user_restricted = "PROXY TCP4 127.0.0.1 255.255.255.255 65535 65535\r\n\0\021ClickHouse client\024\r\253\251\003\0\007user123\0\004\001\0\001\0\0\t0.0.0.0:0\001\tmilovidov\021milovidov-desktop\21ClickHouse client\024\r\253\251\003\0\001\0\0\0\002\001\025SELECT 'Hello, world'\002\0\247\203\254l\325\\z|\265\254F\275\333\206\342\024\202\024\0\0\0\n\0\0\0\240\01\0\02\377\377\377\377\0\0\0"
    assert (
        netcat(
            server.ip_address, 9100, bytearray(data_user_restricted, "latin-1")
        ).find(bytearray("Exception: user123: Authentication failed", "latin-1"))
        >= 0
    )
