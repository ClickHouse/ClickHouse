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

        # Instance with keeper-based storage (node 1)
        cluster.add_instance(
            "node_with_keeper",
            main_configs=["configs/config.d/handlers_with_zookeeper.xml"],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
        )

        # Instance with keeper-based storage (node 2)
        cluster.add_instance(
            "node_with_keeper_2",
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
        node, "h_prefix", "/pfx_test", "SELECT 'prefix_ok'", url_type="PREFIX"
    )

    text, code = http_get(node, "/pfx_test/sub/path")
    assert text == "prefix_ok"
    assert code == 200

    # Exact different path should not match
    _, code = http_get(node, "/other_path")
    assert code != 200 or "prefix_ok" not in _

    drop_handler(node, "h_prefix")


def test_local_url_regexp(cluster):
    """REGEXP URL matching."""
    node = cluster.instances["node"]

    create_handler(
        node,
        "h_regexp",
        "^/re_test/v[0-9]+$",
        "SELECT 'regexp_ok'",
        url_type="REGEXP",
    )

    text, code = http_get(node, "/re_test/v3")
    assert text == "regexp_ok"
    assert code == 200

    # Non-matching path
    _, code = http_get(node, "/re_test/abc")
    assert code != 200 or "regexp_ok" not in _

    drop_handler(node, "h_regexp")


def test_local_method_filtering(cluster):
    """Method filtering — POST-only handler should not match GET."""
    node = cluster.instances["node"]

    create_handler(
        node, "h_post_only", "/method_test", "SELECT 'post_only'", methods=["POST"]
    )

    # POST should work
    text, code = http_post(node, "/method_test")
    assert text == "post_only"
    assert code == 200

    # GET should NOT match
    text, code = http_get(node, "/method_test")
    assert "post_only" not in text

    drop_handler(node, "h_post_only")


def test_local_multi_methods(cluster):
    """Handler with multiple methods (GET, POST)."""
    node = cluster.instances["node"]

    create_handler(
        node,
        "h_multi",
        "/multi_method",
        "SELECT 'multi_ok'",
        methods=["GET", "POST"],
    )

    text_get, _ = http_get(node, "/multi_method")
    text_post, _ = http_post(node, "/multi_method")
    assert text_get == "multi_ok"
    assert text_post == "multi_ok"

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
# Keeper-based storage tests
# ---------------------------------------------------------------------------


def test_keeper_create_and_restart(cluster):
    """CREATE on keeper node → restart both nodes → verify handler persists on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(node1, "h_keeper_1", "/keeper_test_1", "SELECT 'keeper_ok'")

    # Verify on node1
    text, code = http_get(node1, "/keeper_test_1")
    assert text == "keeper_ok"
    assert code == 200

    # Restart both nodes
    node1.restart_clickhouse()
    node2.restart_clickhouse()

    # Verify persistence on node1 after restart
    text, code = http_get(node1, "/keeper_test_1")
    assert text == "keeper_ok", f"Handler lost after restart on node1: got '{text}'"
    assert code == 200

    # Verify cross-node propagation: node2 should also serve the handler
    text, code = http_get(node2, "/keeper_test_1")
    assert text == "keeper_ok", f"Handler not propagated to node2: got '{text}'"
    assert code == 200

    drop_handler(node1, "h_keeper_1")


def test_keeper_alter_and_restart(cluster):
    """ALTER on keeper node → restart both → verify altered state persists on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(node1, "h_keeper_alter", "/keeper_alter", "SELECT 10")

    text, _ = http_get(node1, "/keeper_alter")
    assert text == "10"

    alter_handler(node1, "h_keeper_alter", query="SELECT 20")

    text, _ = http_get(node1, "/keeper_alter")
    assert text == "20"

    # Restart both nodes
    node1.restart_clickhouse()
    node2.restart_clickhouse()

    # Verify altered state persists on node1
    text, _ = http_get(node1, "/keeper_alter")
    assert text == "20", f"ALTER lost after restart on node1: got '{text}'"

    # Verify altered state propagated to node2
    text, _ = http_get(node2, "/keeper_alter")
    assert text == "20", f"ALTER not propagated to node2: got '{text}'"

    drop_handler(node1, "h_keeper_alter")


def test_keeper_drop_and_restart(cluster):
    """DROP on keeper node → restart both → verify handler stays gone on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(node1, "h_keeper_drop", "/keeper_drop", "SELECT 'should_be_gone'")

    text, _ = http_get(node1, "/keeper_drop")
    assert text == "should_be_gone"

    drop_handler(node1, "h_keeper_drop")

    _, code = http_get(node1, "/keeper_drop")
    assert code != 200 or "should_be_gone" not in _

    # Restart both nodes
    node1.restart_clickhouse()
    node2.restart_clickhouse()

    # Still gone on node1
    _, code = http_get(node1, "/keeper_drop")
    assert code != 200 or "should_be_gone" not in _

    # Still gone on node2
    _, code = http_get(node2, "/keeper_drop")
    assert code != 200 or "should_be_gone" not in _


def test_keeper_url_prefix_restart(cluster):
    """PREFIX handler with keeper storage → restart both → verify on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(
        node1, "h_keeper_pfx", "/keeper_pfx", "SELECT 'kpfx'", url_type="PREFIX"
    )

    text, _ = http_get(node1, "/keeper_pfx/sub")
    assert text == "kpfx"

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    text, _ = http_get(node1, "/keeper_pfx/sub")
    assert text == "kpfx", f"PREFIX handler lost after restart on node1: got '{text}'"

    text, _ = http_get(node2, "/keeper_pfx/sub")
    assert text == "kpfx", f"PREFIX handler not propagated to node2: got '{text}'"

    drop_handler(node1, "h_keeper_pfx")


def test_keeper_url_regexp_restart(cluster):
    """REGEXP handler with keeper storage → restart both → verify on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(
        node1,
        "h_keeper_re",
        "^/keeper_re/v[0-9]+$",
        "SELECT 'kre'",
        url_type="REGEXP",
    )

    text, _ = http_get(node1, "/keeper_re/v5")
    assert text == "kre"

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    text, _ = http_get(node1, "/keeper_re/v5")
    assert text == "kre", f"REGEXP handler lost after restart on node1: got '{text}'"

    text, _ = http_get(node2, "/keeper_re/v5")
    assert text == "kre", f"REGEXP handler not propagated to node2: got '{text}'"

    drop_handler(node1, "h_keeper_re")


def test_keeper_method_filtering_restart(cluster):
    """Method-filtered handler with keeper storage → restart both → verify on both."""
    node1 = cluster.instances["node_with_keeper"]
    node2 = cluster.instances["node_with_keeper_2"]

    create_handler(
        node1,
        "h_keeper_post",
        "/keeper_post",
        "SELECT 'kpost'",
        methods=["POST"],
    )

    text, _ = http_post(node1, "/keeper_post")
    assert text == "kpost"

    # GET should not match
    text, _ = http_get(node1, "/keeper_post")
    assert "kpost" not in text

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    # POST still works after restart on node1
    text, _ = http_post(node1, "/keeper_post")
    assert text == "kpost", f"POST handler lost after restart on node1: got '{text}'"

    # POST works on node2 (cross-node propagation)
    text, _ = http_post(node2, "/keeper_post")
    assert text == "kpost", f"POST handler not propagated to node2: got '{text}'"

    # GET still doesn't match on either node
    text, _ = http_get(node1, "/keeper_post")
    assert "kpost" not in text
    text, _ = http_get(node2, "/keeper_post")
    assert "kpost" not in text

    drop_handler(node1, "h_keeper_post")
