import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

# Two upstream servers. Both listen on a PROXY-protocol port (9010) because the proxy always
# prepends a PROXY protocol v1 header to the upstream connection.
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_proxy_protocol.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_proxy_protocol.xml"],
    stay_alive=True,
)

PROXY_PORT = 9100
PROXY_CONFIG = "/etc/clickhouse-proxy.xml"


def run_via_proxy(sql, database=None, port=PROXY_PORT):
    """Run clickhouse-client inside node1's container, connecting through the proxy.

    Returns a (exit_code, combined_output) tuple."""
    db = f"--database {database} " if database else ""
    cmd = (
        f'timeout 30 clickhouse client --host 127.0.0.1 --port {port} '
        f'{db}--query "{sql}"; echo EXIT_CODE=$?'
    )
    out = node1.exec_in_container(["bash", "-c", cmd], user="root", nothrow=True)
    exit_code = None
    body_lines = []
    for line in out.splitlines():
        if line.startswith("EXIT_CODE="):
            exit_code = int(line[len("EXIT_CODE=") :])
        else:
            body_lines.append(line)
    return exit_code, "\n".join(body_lines)


def start_proxy(config_container_path):
    node1.exec_in_container(
        ["clickhouse", "proxy", "--config-file=" + config_container_path],
        detach=True,
        user="root",
    )


def wait_for_proxy(retries=30):
    last = ""
    for _ in range(retries):
        code, out = run_via_proxy("SELECT 1")
        if code == 0 and out.strip() == "1":
            return
        last = out
        time.sleep(1)
    raise Exception(f"proxy did not become ready, last output: {last}")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # A per-node marker so a query routed through the proxy reveals which upstream served it.
        for node in (node1, node2):
            node.query("CREATE TABLE default.node_marker (name String) ENGINE = Memory")
            node.query(f"INSERT INTO default.node_marker VALUES ('{node.name}')")

        # The database used to route to cluster_b must exist on that upstream.
        node2.query("CREATE DATABASE route_b")

        node1.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs", "proxy_config.xml"), PROXY_CONFIG
        )
        start_proxy(PROXY_CONFIG)
        wait_for_proxy()

        yield cluster
    finally:
        cluster.shutdown()


def test_native_query_through_proxy(started_cluster):
    code, out = run_via_proxy("SELECT 6 * 7")
    assert code == 0, out
    assert out.strip() == "42", out


def test_default_route_lands_on_cluster_a(started_cluster):
    code, out = run_via_proxy("SELECT name FROM default.node_marker")
    assert code == 0, out
    assert out.strip() == "node1", out


def test_routing_by_database(started_cluster):
    # Connecting with the `route_b` database must be routed to cluster_b (node2).
    code, out = run_via_proxy("SELECT name FROM default.node_marker", database="route_b")
    assert code == 0, out
    assert out.strip() == "node2", out


def test_large_result_relayed_intact(started_cluster):
    # Exercises the raw byte relay for a payload much larger than the handshake.
    code, out = run_via_proxy("SELECT count(), sum(number) FROM numbers(1000000)")
    assert code == 0, out
    assert out.strip() == "1000000\t499999500000", out


def test_reject_rule_closes_connection(started_cluster):
    # A `reject` rule must drop the connection instead of routing it.
    code, out = run_via_proxy("SELECT 1", database="forbidden")
    assert code != 0, f"expected the rejected connection to fail, got: {out}"


def test_invalid_upstream_config_fails_at_startup(started_cluster):
    # A replica without a tcp_port must make the proxy fail at startup, not at connection time.
    node1.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs", "proxy_bad_config.xml"),
        "/etc/clickhouse-proxy-bad.xml",
    )
    out = node1.exec_in_container(
        [
            "bash",
            "-c",
            "timeout 30 clickhouse proxy --config-file=/etc/clickhouse-proxy-bad.xml 2>&1; "
            "echo EXIT_CODE=$?",
        ],
        user="root",
        nothrow=True,
    )
    assert "EXIT_CODE=0" not in out, out
    assert "tcp_port" in out, out
    assert "INVALID_CONFIG_PARAMETER" in out, out
