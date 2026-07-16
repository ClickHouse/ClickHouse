import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_memory_database():
    node.query("CREATE DATABASE test ENGINE Memory")
    node.query("CREATE TABLE test.test_table(a String) ENGINE Memory")
    node.query("DROP DATABASE test")
    node.restart_clickhouse(kill=True)
    assert node.query("SHOW DATABASES LIKE 'test'").strip() == ""


def test_flushes_async_insert_queue():
    node.query(
        """
    CREATE TABLE flush_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a;
    SET async_insert = 1;
    SET wait_for_async_insert = 0;
    SET async_insert_busy_timeout_ms = 1000000;
    INSERT INTO flush_test VALUES ('world', 23456);
    """
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM flush_test") == "world\t23456\n"


def test_server_startup_notify_socket_exception():
    # Regression test.
    # Make sure after an exception is thrown from systemdNotify during startup
    # the server does not segfault on further queries.
    node.stop_clickhouse()
    node.start_clickhouse(environment={"NOTIFY_SOCKET": "bad"}, wait_start=False)

    node.wait_for_log_line("Shut down storages", timeout=180, look_behind_lines=1)
    assert "Connection refused" in node.query_and_get_error("SELECT 1")

    node.start_clickhouse()
