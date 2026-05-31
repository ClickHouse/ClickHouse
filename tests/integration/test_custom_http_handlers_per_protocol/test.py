import urllib.parse
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance(
    "server",
    base_config_dir="configs",
)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def http_query(host, port, path="/", query=None):
    """Send an HTTP request and return the response body."""
    if query:
        url = f"http://{host}:{port}{path}?query={urllib.parse.quote(query)}"
    else:
        url = f"http://{host}:{port}{path}"
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request).read()
    return response.decode("utf-8").strip()


def test_default_port_uses_default_handlers():
    """Port 8123 should use the default http_handlers config."""
    result = http_query(server.ip_address, 8123, "/ping")
    assert result == "default_pong"


def test_alt_port_uses_alt_handlers():
    """Port 8124 should use http_handlers_alt config."""
    result = http_query(server.ip_address, 8124, "/ping")
    assert result == "alt_pong"


def test_alt_port_custom_endpoint():
    """Port 8124 should serve the /custom endpoint defined in http_handlers_alt."""
    result = http_query(server.ip_address, 8124, "/custom")
    assert result == "custom_handler_works"


def test_default_port_no_custom_endpoint():
    """Port 8123 should NOT serve the /custom endpoint (only in alt handlers)."""
    try:
        http_query(server.ip_address, 8123, "/custom")
        assert False, "Expected HTTP error for /custom on default port"
    except urllib.error.HTTPError as e:
        # 400 or 404 expected — the default handlers don't have a /custom rule
        assert e.code in (400, 404), f"Unexpected HTTP error code: {e.code}"


def test_regular_queries_work_on_both_ports():
    """Both ports should handle regular SQL queries via the <defaults/> handler."""
    result_default = http_query(server.ip_address, 8123, query="SELECT 1")
    assert result_default == "1"

    result_alt = http_query(server.ip_address, 8124, query="SELECT 1")
    assert result_alt == "1"
