import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_session_with_reconnect(queries_before, queries_after, out_file):
    """Run one clickhouse-client session that pauses between two batches of
    queries; the server is restarted during the pause, forcing a reconnect."""
    before = "".join(q + ";\n" for q in queries_before)
    after = "".join(q + ";\n" for q in queries_after)
    script = (
        f"( printf '{before}'; sleep 12; printf '{after}' ) | "
        "clickhouse-client -mn --allow_experimental_table_namespaces=1 "
        f"> {out_file} 2>&1"
    )
    node.exec_in_container(["bash", "-c", script], detach=True)


def test_reconnect_preserves_namespace_scope(started_cluster):
    """
    After a mid-session reconnect, an unqualified name must resolve in the
    selected namespace or fail - never in the parent database (which holds a
    decoy table with different contents).
    """
    node.query("DROP DATABASE IF EXISTS reconns")
    node.query("CREATE DATABASE reconns")
    node.query("CREATE TABLE reconns.t (x Int32) ENGINE = Memory")
    node.query("INSERT INTO reconns.t VALUES (100)")
    node.query("CREATE TABLE reconns.`ns.t` (x Int32) ENGINE = Memory")
    node.query("INSERT INTO reconns.`ns.t` VALUES (1)")
    node.query("CREATE TABLE reconns.`ns1.ns2.t` (x Int32) ENGINE = Memory")
    node.query("INSERT INTO reconns.`ns1.ns2.t` VALUES (2)")

    run_session_with_reconnect(
        ["USE reconns.ns"], ["SELECT sum(x) FROM t"], "/tmp/reconnect_flat.out"
    )
    run_session_with_reconnect(
        ["USE reconns.ns1.ns2"], ["SELECT sum(x) FROM t"], "/tmp/reconnect_nested.out"
    )
    time.sleep(3)
    node.restart_clickhouse()
    time.sleep(14)

    flat = node.exec_in_container(["cat", "/tmp/reconnect_flat.out"])
    nested = node.exec_in_container(["cat", "/tmp/reconnect_nested.out"])

    # the scope was either restored (scoped values) or the query failed loudly;
    # the parent decoy value must never appear
    assert "100" not in flat, f"reconnect leaked into the parent database: {flat}"
    assert "1" in flat or "Exception" in flat or "error" in flat.lower(), f"unexpected output: {flat}"

    assert "100" not in nested, f"nested reconnect leaked into the parent database: {nested}"
    assert "2" in nested or "Exception" in nested or "error" in nested.lower(), f"unexpected output: {nested}"

    node.query("DROP DATABASE reconns")
