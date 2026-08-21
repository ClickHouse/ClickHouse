import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/introspection.xml", "configs/startup_scripts.xml"],
)

INTROSPECTION_PORT = 9010


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

    node.query("SYSTEM STOP LISTEN CUSTOM 'introspection_native'")
    assert introspection_client().query("SELECT 1") == "1\n"

    node.query("SYSTEM STOP LISTEN QUERIES ALL")
    assert "Connection refused" in node.query_and_get_error("SELECT 1")
    assert introspection_client().query("SELECT 1") == "1\n"
    introspection_client().query("SYSTEM START LISTEN QUERIES ALL")
    assert node.query("SELECT 1") == "1\n"
