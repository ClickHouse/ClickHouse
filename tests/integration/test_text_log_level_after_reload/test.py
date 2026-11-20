import uuid

import pytest

from helpers.cluster import ClickHouseCluster

# The file log is capped at `information`, text_log wants `trace`.
# The root logger level must stay at `trace` (the most verbose sink), both at
# startup and after the levels are re-applied on a configuration reload.
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/logger.xml"])
# Same, with the synchronous split channel.
node_sync = cluster.add_instance(
    "node_sync", main_configs=["configs/logger.xml", "configs/logger_sync.xml"]
)


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


LOG_FILE = "/var/log/clickhouse-server/clickhouse-server.log"


def file_log_size(node):
    return int(node.exec_in_container(["bash", "-c", f"stat -c %s {LOG_FILE}"]))


def file_log_since(node, offset):
    return node.exec_in_container(["bash", "-c", f"tail -c +{offset + 1} {LOG_FILE}"])


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

    log_offset = file_log_size(node)
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
    log = file_log_since(node, log_offset)
    assert "<Trace>" not in log
    assert "<Debug>" not in log


def set_logger_config(node, logger_level, text_log_level):
    text_log_level_xml = (
        "" if text_log_level is None else f"<level>{text_log_level}</level>"
    )
    node.replace_config(
        "/etc/clickhouse-server/config.d/logger.xml",
        f"""<clickhouse>
    <logger>
        <level>{logger_level}</level>
        <log>{LOG_FILE}</log>
    </logger>
    <text_log>
        {text_log_level_xml}
    </text_log>
</clickhouse>""",
    )
    node.query("SYSTEM RELOAD CONFIG")


def restore_logger_config(node):
    set_logger_config(node, "information", "trace")


def check_text_log_trace(node, expected):
    log_offset = file_log_size(node)
    since = now(node)
    for _ in range(5):
        node.query("SELECT 1")
    rows = trace_rows_since(node, since)
    assert (rows > 0) == expected, f"Trace rows in text_log: {rows}"
    # The file log stays at `trace`, so the root logger keeps producing Trace messages and only
    # the text_log sink's own threshold decides whether they reach system.text_log.
    assert "<Trace>" in file_log_since(node, log_offset)


@pytest.mark.parametrize("instance", [node, node_sync], ids=["async", "sync"])
def test_text_log_level_is_reloadable(start_cluster, instance):
    try:
        set_logger_config(instance, "trace", "information")
        check_text_log_trace(instance, False)

        set_logger_config(instance, "trace", "trace")
        check_text_log_trace(instance, True)

        set_logger_config(instance, "trace", "none")
        check_text_log_trace(instance, False)

        # Removing the explicit level restores the default `trace` threshold, as after a restart.
        set_logger_config(instance, "trace", None)
        check_text_log_trace(instance, True)
    finally:
        restore_logger_config(instance)


@pytest.mark.parametrize("instance", [node, node_sync], ids=["async", "sync"])
def test_flush_after_reload_to_none(start_cluster, instance):
    # Messages accepted while text_log was enabled must still be flushed after it is reloaded to `none`.
    try:
        marker = uuid.uuid4().hex
        instance.query(f"SELECT '{marker}' FORMAT Null")
        set_logger_config(instance, "information", "none")
        instance.query("SYSTEM FLUSH LOGS text_log")
        assert (
            int(
                instance.query(
                    f"SELECT count() FROM system.text_log WHERE message LIKE '%{marker}%'"
                )
            )
            > 0
        )
    finally:
        restore_logger_config(instance)
