import os
import uuid

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

node = cluster.add_instance(
    "node",
    main_configs=["configs/introspection.xml", "configs/startup_scripts.xml"],
)
node_reload = cluster.add_instance(
    "node_reload",
    main_configs=[
        "configs/introspection.xml",
        "configs/introspection_ipv6.xml",
        "configs/node_reload.xml",
    ],
)

INTROSPECTION_PORT = 9010
INTROSPECTION_CONFIG = "/etc/clickhouse-server/config.d/introspection.xml"
NODE_RELOAD_CONFIG = "/etc/clickhouse-server/config.d/node_reload.xml"
RESTART_WARNING = "which is not reconfigured on config reload"
with open(os.path.join(SCRIPT_DIR, "configs/introspection.xml")) as f:
    INTROSPECTION_CONFIG_CONTENT = f.read()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def introspection_client():
    return Client(node.ip_address, INTROSPECTION_PORT, command=cluster.client_bin_path)


def test_introspection_port(started_cluster):
    # Verify that introspection port runs before startup scripts finish.
    assert_eq_with_retry(
        introspection_client(),
        "SELECT count() FROM system.processes"
        " WHERE Settings['log_comment'] = 'introspection_test_startup_script'",
        "1",
    )
    assert "QUERY_IS_PROHIBITED" in introspection_client().query_and_get_error(
        "SYSTEM RELOAD CONFIG"
    )

    introspection_client().query(
        "KILL QUERY WHERE Settings['log_comment'] = 'introspection_test_startup_script'"
    )
    assert_eq_with_retry(node, "SELECT 1", "1")

    assert_eq_with_retry(introspection_client(), "SYSTEM RELOAD CONFIG", "")

    assert "AUTHENTICATION_FAILED" in introspection_client().query_and_get_error(
        "SELECT 1", password="invalid"
    )

    assert "QUERY_IS_PROHIBITED" in introspection_client().query_and_get_error(
        "CREATE TABLE t (a UInt8) ENGINE = Memory"
    )
    assert "QUERY_IS_PROHIBITED" in introspection_client().query_and_get_error(
        "INSERT INTO system.one VALUES (1)"
    )
    assert introspection_client().query("EXISTS TABLE system.one") == "1\n"
    introspection_client().query("SHOW PROCESSLIST")

    # Reaches the interpreter and fails on its argument instead of being refused.
    # A fresh id per run keeps the kind assertion below scoped to this execution.
    describe_cache_query_id = f"introspection_test_describe_cache_{uuid.uuid4()}"
    assert "There is no cache by name" in introspection_client().query_and_get_error(
        "DESCRIBE FILESYSTEM CACHE 'nonexistent'", query_id=describe_cache_query_id
    )
    # Impersonating another user is not a diagnostic query, so it stays refused.
    # On a regular port the same statement reports UNKNOWN_USER instead.
    assert "QUERY_IS_PROHIBITED" in introspection_client().query_and_get_error(
        "EXECUTE AS foo"
    )
    # It is admitted as a DESCRIBE, not as some other kind that the port also accepts.
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        "SELECT DISTINCT query_kind FROM system.query_log"
        f" WHERE query_id = '{describe_cache_query_id}'",
        "Describe",
    )

    node.query("SYSTEM STOP LISTEN CUSTOM 'introspection_native'")
    assert introspection_client().query("SELECT 1") == "1\n"

    node.query("SYSTEM STOP LISTEN QUERIES ALL")
    assert "Connection refused" in node.query_and_get_error("SELECT 1")
    assert introspection_client().query("SELECT 1") == "1\n"
    introspection_client().query("SYSTEM START LISTEN QUERIES ALL")
    assert node.query("SELECT 1") == "1\n"


@pytest.mark.parametrize(
    "endpoint, port",
    [
        # The introspection flag is removed from the running endpoint.
        ("introspection_native", INTROSPECTION_PORT),
        # The same, and the endpoint moves to a free port.
        ("introspection_native", 9013),
        # The introspection endpoint is replaced by a regular one on its port.
        ("native_on_introspection_port", INTROSPECTION_PORT),
    ],
)
def test_reload_with_regular_endpoint_on_introspection_port(
    started_cluster, endpoint, port
):
    client = Client(
        node_reload.ip_address, INTROSPECTION_PORT, command=cluster.client_bin_path
    )
    warnings = int(node_reload.count_in_log(RESTART_WARNING))
    try:
        node_reload.replace_config(
            INTROSPECTION_CONFIG,
            f"<clickhouse><protocols><{endpoint}><type>tcp</type><port>{port}</port></{endpoint}></protocols></clickhouse>",
        )
        # The introspection listener is not reconfigured on reload and keeps its address until restart.
        for query in [
            "SYSTEM RELOAD CONFIG",
            f"SYSTEM START LISTEN CUSTOM '{endpoint}'",
        ]:
            node_reload.query(query)
            warnings_after = int(node_reload.count_in_log(RESTART_WARNING))
            assert warnings_after > warnings, query
            warnings = warnings_after
        assert "QUERY_IS_PROHIBITED" in client.query_and_get_error(
            "CREATE TABLE t (a UInt8) ENGINE = Memory"
        )
    finally:
        node_reload.replace_config(INTROSPECTION_CONFIG, INTROSPECTION_CONFIG_CONTENT)
        node_reload.query("SYSTEM RELOAD CONFIG")


def test_reload_marking_regular_endpoint_as_introspection(started_cluster):
    warning = "Protocol 'regular_native' is marked as introspection"
    warnings_before = int(node_reload.count_in_log(warning))
    try:
        node_reload.replace_in_config(
            NODE_RELOAD_CONFIG,
            "<type>tcp</type>",
            "<type>tcp</type><introspection>true</introspection>",
        )
        # Introspection listeners are created only at startup, so the running regular listener is kept.
        node_reload.query("SYSTEM RELOAD CONFIG")
        assert int(node_reload.count_in_log(warning)) > warnings_before
        Client(node_reload.ip_address, 9011, command=cluster.client_bin_path).query(
            "INSERT INTO FUNCTION null('a UInt8') SELECT 1"
        )
    finally:
        node_reload.replace_in_config(
            NODE_RELOAD_CONFIG, "<introspection>true</introspection>", ""
        )
        node_reload.query("SYSTEM RELOAD CONFIG")


@pytest.mark.parametrize("port_name", ["grpc_port", "arrowflight_port"])
def test_reload_adding_grpc_port_on_introspection_port(started_cluster, port_name):
    # gRPC treats `0.0.0.0` as a dual-stack wildcard, so it overlaps the introspection listener on `::`.
    warning = f"for {port_name}: the address is used by the introspection listener"
    port_config = f"<{port_name}>9012</{port_name}>"
    try:
        node_reload.replace_in_config(
            NODE_RELOAD_CONFIG,
            "<listen_try>0</listen_try>",
            f"<listen_try>0</listen_try>{port_config}",
        )
        node_reload.query("SYSTEM RELOAD CONFIG")
        assert node_reload.contains_in_log(warning)
    finally:
        node_reload.replace_in_config(NODE_RELOAD_CONFIG, port_config, "")
        node_reload.query("SYSTEM RELOAD CONFIG")
