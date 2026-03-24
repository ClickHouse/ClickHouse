"""Integration tests for SQL-defined HTTP handlers (CREATE/ALTER/DROP HANDLER).

Tests local-disk persistence and keeper-based storage, including
restart round-trips, URL matching (exact/prefix/regexp), method
filtering, ALTER semantics, and DROP removal.
"""

import logging
import pytest
import requests

from helpers.cluster import ClickHouseCluster

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        # Instance with local storage (no keeper)
        cluster.add_instance(
            "node",
            main_configs=["configs/config.d/handlers.xml"],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )

        # Instance with zookeeper config (tests local-disk fallback)
        cluster.add_instance(
            "node_with_keeper",
            main_configs=["configs/config.d/handlers_with_zookeeper.xml"],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def http_get(node, path):
    """Issue an HTTP GET to the given path on the node's HTTP port."""
    url = f"http://{node.ip_address}:8123{path}"
    resp = requests.get(url)
    return resp.text.strip(), resp.status_code


def http_post(node, path, body=""):
    """Issue an HTTP POST to the given path on the node's HTTP port."""
    url = f"http://{node.ip_address}:8123{path}"
    resp = requests.post(url, data=body)
    return resp.text.strip(), resp.status_code


def create_handler(node, name, url, query, methods=None, url_type=""):
    """Helper to CREATE HANDLER via SQL."""
    methods_clause = ""
    if methods:
        methods_clause = f"METHODS ({', '.join(methods)})"
    url_type_clause = f" {url_type}" if url_type else ""
    sql = f"CREATE HANDLER {name} URL{url_type_clause} '{url}' {methods_clause} AS '{query}'"
    node.query(sql)


def drop_handler(node, name, if_exists=False):
    """Helper to DROP HANDLER via SQL."""
    ie = "IF EXISTS " if if_exists else ""
    node.query(f"DROP HANDLER {ie}{name}")


def alter_handler(node, name, query=None, url=None, url_type="", methods=None):
    """Helper to ALTER HANDLER via SQL."""
    parts = [f"ALTER HANDLER {name}"]
    if url is not None:
        url_type_clause = f" {url_type}" if url_type else ""
        parts.append(f"URL{url_type_clause} '{url}'")
    if methods is not None:
        parts.append(f"METHODS ({', '.join(methods)})")
    if query is not None:
        parts.append(f"AS '{query}'")
    node.query(" ".join(parts))


# ---------------------------------------------------------------------------
# Local storage tests
# ---------------------------------------------------------------------------


def test_local_create_drop(cluster):
    """CREATE HANDLER → verify HTTP → DROP → verify gone."""
    node = cluster.instances["node"]

    create_handler(node, "h_local_1", "/test_local_1", "SELECT 42 AS answer")
    text, code = http_get(node, "/test_local_1")
    assert text == "42", f"Expected '42', got '{text}'"
    assert code == 200

    drop_handler(node, "h_local_1")
    _, code = http_get(node, "/test_local_1")
    assert code != 200 or "42" not in _


def test_local_persistence_restart(cluster):
    """CREATE HANDLER → restart → verify handler survives restart → DROP → restart → verify gone."""
    node = cluster.instances["node"]

    create_handler(node, "h_persist", "/test_persist", "SELECT 101 AS persisted")

    # Verify before restart
    text, _ = http_get(node, "/test_persist")
    assert text == "101"

    # Restart
    node.restart_clickhouse()

    # Verify after restart (persistence check)
    text, code = http_get(node, "/test_persist")
    assert text == "101", f"Handler did not survive restart: got '{text}'"
    assert code == 200

    # DROP and restart
    drop_handler(node, "h_persist")
    node.restart_clickhouse()

    # Verify gone after restart
    _, code = http_get(node, "/test_persist")
    assert code != 200 or "101" not in _


def test_local_alter_persistence(cluster):
    """CREATE → ALTER → restart → verify altered query persists."""
    node = cluster.instances["node"]

    create_handler(node, "h_alter_p", "/test_alter_p", "SELECT 1 AS before_alter")
    text, _ = http_get(node, "/test_alter_p")
    assert text == "1"

    alter_handler(node, "h_alter_p", query="SELECT 2 AS after_alter")
    text, _ = http_get(node, "/test_alter_p")
    assert text == "2"

    # Restart — altered state must persist
    node.restart_clickhouse()

    text, _ = http_get(node, "/test_alter_p")
    assert text == "2", f"ALTER did not survive restart: got '{text}'"

    drop_handler(node, "h_alter_p")


def test_local_url_prefix(cluster):
    """PREFIX URL matching."""
    node = cluster.instances["node"]

    create_handler(
        node, "h_prefix", "/pfx_test", "SELECT 201 AS pfx", url_type="PREFIX"
    )

    text, code = http_get(node, "/pfx_test/sub/path")
    assert text == "201"
    assert code == 200

    # Exact different path should not match
    _, code = http_get(node, "/other_path")
    assert code != 200 or "201" not in _

    drop_handler(node, "h_prefix")


def test_local_url_regexp(cluster):
    """REGEXP URL matching."""
    node = cluster.instances["node"]

    create_handler(
        node,
        "h_regexp",
        "^/re_test/v[0-9]+$",
        "SELECT 202 AS re",
        url_type="REGEXP",
    )

    text, code = http_get(node, "/re_test/v3")
    assert text == "202"
    assert code == 200

    # Non-matching path
    _, code = http_get(node, "/re_test/abc")
    assert code != 200 or "202" not in _

    drop_handler(node, "h_regexp")


def test_local_method_filtering(cluster):
    """Method filtering — POST-only handler should not match GET."""
    node = cluster.instances["node"]

    create_handler(
        node, "h_post_only", "/method_test", "SELECT 203 AS post", methods=["POST"]
    )

    # POST should work
    text, code = http_post(node, "/method_test")
    assert text == "203"
    assert code == 200

    # GET should NOT match
    text, code = http_get(node, "/method_test")
    assert "203" not in text

    drop_handler(node, "h_post_only")


def test_local_multi_methods(cluster):
    """Handler with multiple methods (GET, POST)."""
    node = cluster.instances["node"]

    create_handler(
        node,
        "h_multi",
        "/multi_method",
        "SELECT 204 AS multi",
        methods=["GET", "POST"],
    )

    text_get, _ = http_get(node, "/multi_method")
    text_post, _ = http_post(node, "/multi_method")
    assert text_get == "204"
    assert text_post == "204"

    drop_handler(node, "h_multi")


def test_local_if_not_exists(cluster):
    """CREATE IF NOT EXISTS should not throw on duplicate."""
    node = cluster.instances["node"]

    create_handler(node, "h_ine", "/ine_test", "SELECT 1")
    # Should not throw
    node.query(
        "CREATE HANDLER IF NOT EXISTS h_ine URL '/ine_test' AS 'SELECT 1'"
    )

    drop_handler(node, "h_ine")


def test_local_drop_if_exists(cluster):
    """DROP IF EXISTS should not throw on non-existent."""
    node = cluster.instances["node"]
    # Should not throw
    drop_handler(node, "h_nonexistent_xyz", if_exists=True)


def test_local_drop_nonexistent_throws(cluster):
    """DROP on non-existent handler should throw."""
    node = cluster.instances["node"]
    with pytest.raises(Exception, match="BAD_ARGUMENTS"):
        drop_handler(node, "h_nonexistent_xyz")


def test_local_alter_if_exists_nonexistent(cluster):
    """ALTER IF EXISTS on non-existent handler should not throw."""
    node = cluster.instances["node"]
    # Should not throw
    node.query(
        "ALTER HANDLER IF EXISTS h_nonexistent_xyz URL '/foo' AS 'SELECT 1'"
    )


# ---------------------------------------------------------------------------
# Keeper config fallback tests
# ---------------------------------------------------------------------------
# ZooKeeper-based handler storage is not yet implemented; the server falls
# back to local-disk storage when custom_handlers_storage.type = zookeeper.
# These tests verify the fallback works: the node starts, handlers persist
# locally, and survive restart.


def test_keeper_config_fallback_persistence(cluster):
    """Node with zookeeper config falls back to local storage and persists handlers."""
    node1 = cluster.instances["node_with_keeper"]

    create_handler(node1, "h_fallback", "/fallback_test", "SELECT 205 AS fb")

    text, code = http_get(node1, "/fallback_test")
    assert text == "205"
    assert code == 200

    # Restart — handler should survive via local-disk fallback
    node1.restart_clickhouse()

    text, code = http_get(node1, "/fallback_test")
    assert text == "205", f"Handler lost after restart: got '{text}'"
    assert code == 200

    drop_handler(node1, "h_fallback")
