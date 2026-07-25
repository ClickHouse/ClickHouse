import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_session_with_reconnect(database, queries_after, out_file):
    """Run one clickhouse-client session bound to a namespace scope that sends its
    queries only after the server has gone down and come back, so the scope must
    survive the reconnect handshake. The client runs through a link named without
    "clickhouse", otherwise the restart's pkill would kill it."""
    after = "".join(q + ";\n" for q in queries_after)
    script = (
        "ln -sf $(command -v clickhouse) /tmp/chc; "
        "( while /tmp/chc -q 'SELECT 1' >/dev/null 2>&1; do sleep 0.5; done; "
        "  while ! /tmp/chc -q 'SELECT 1' >/dev/null 2>&1; do sleep 0.5; done; "
        f"  printf '{after}' ) | "
        "/tmp/chc --allow_experimental_table_namespaces=1 --enable_analyzer=1 "
        f"--database '{database}' > {out_file} 2>&1"
    )
    node.exec_in_container(["bash", "-c", script], detach=True)


def test_reconnect_preserves_namespace_scope(started_cluster):
    """
    After a mid-session reconnect, an unqualified name must resolve in the
    selected namespace - never in the parent database (which holds a decoy
    table with different contents).
    """
    node.query("DROP DATABASE IF EXISTS reconns")
    node.query("CREATE DATABASE reconns")
    node.query("CREATE TABLE reconns.t (x Int32) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO reconns.t VALUES (100)")
    node.query("CREATE TABLE reconns.`ns.t` (x Int32) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO reconns.`ns.t` VALUES (1)")
    node.query("CREATE TABLE reconns.`ns1.ns2.t` (x Int32) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO reconns.`ns1.ns2.t` VALUES (2)")

    run_session_with_reconnect(
        "reconns.ns", ["SELECT sum(x) FROM t"], "/tmp/reconnect_flat.out"
    )
    run_session_with_reconnect(
        "reconns.ns1.ns2", ["SELECT sum(x) FROM t"], "/tmp/reconnect_nested.out"
    )
    time.sleep(3)
    node.restart_clickhouse()

    def wait_output(path, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            content = node.exec_in_container(["bash", "-c", f"cat {path} 2>/dev/null"])
            if content.strip():
                return content
            time.sleep(1)
        return node.exec_in_container(["bash", "-c", f"cat {path} 2>/dev/null"])

    flat = wait_output("/tmp/reconnect_flat.out")
    nested = wait_output("/tmp/reconnect_nested.out")

    assert "100" not in flat, f"reconnect leaked into the parent database: {flat}"
    assert flat.strip() == "1", f"scope was not restored after reconnect: {flat}"

    assert "100" not in nested, f"nested reconnect leaked into the parent database: {nested}"
    assert nested.strip() == "2", f"nested scope was not restored after reconnect: {nested}"

    node.query("DROP DATABASE reconns")
