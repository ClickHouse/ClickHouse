import pytest

from helpers.cluster import ClickHouseCluster

# The file log is capped at `information`, text_log wants `trace`.
# The root logger level must stay at `trace` (the most verbose sink), both at
# startup and after the levels are re-applied on a configuration reload.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/logger.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def trace_rows_since(node, since):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            f"SELECT count() FROM system.text_log WHERE level = 'Trace' AND event_time_microseconds > '{since}'"
        )
    )


def now(node):
    return node.query("SELECT now64(6)").strip()


def test_text_log_keeps_trace_after_reload(start_cluster):
    # Startup: the root level must already include text_log.level when the loggers are
    # built, before the first configuration reload, so early Trace messages are kept.
    node.query("SYSTEM FLUSH LOGS")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.text_log WHERE level = 'Trace' AND logger_name = 'Application' AND message LIKE 'Initialized DateLUT%'"
            )
        )
        > 0
    )

    since = now(node)
    for _ in range(5):
        node.query("SELECT 1")
    assert trace_rows_since(node, since) > 0

    # Re-applies the logger levels from the (unchanged) configuration.
    node.query("SYSTEM RELOAD CONFIG")

    since = now(node)
    for _ in range(5):
        node.query("SELECT 1")
    assert trace_rows_since(node, since) > 0

    # The file log must still be capped at `information`.
    log = node.exec_in_container(
        ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"]
    )
    assert "<Trace>" not in log
    assert "<Debug>" not in log
