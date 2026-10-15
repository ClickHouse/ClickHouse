import json
import pytest

from helpers.cluster import ClickHouseCluster
from concurrent.futures import ThreadPoolExecutor

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/logger.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.submit(node.query, "select 1") for _ in range(100))
        yield cluster
    finally:
        cluster.shutdown()


def test_async_logger_profile_events(start_cluster):
    events = json.loads(
        node.query(
            """
        SELECT event, value
        FROM system.events
        WHERE name ILIKE '%asynclog%'
        FORMAT JSON
        """
        )
    )["data"]

    # At startup we should have logged heavily and concurrently in console, text_log and file_log
    # wo we should have metrics for both total and dropped messages
    console_total = next(
        (x for x in events if x["event"] == "AsyncLoggingConsoleTotalMessages"), None
    )
    console_dropped = next(
        (x for x in events if x["event"] == "AsyncLoggingConsoleDroppedMessages"), None
    )
    assert console_total is not None
    assert console_dropped is not None
    assert int(console_total["value"]) >= int(console_dropped["value"])

    file_log_total = next(
        (x for x in events if x["event"] == "AsyncLoggingFileLogTotalMessages"), None
    )
    file_log_dropped = next(
        (x for x in events if x["event"] == "AsyncLoggingFileLogDroppedMessages"), None
    )
    assert file_log_total is not None
    assert file_log_dropped is not None
    assert int(file_log_total["value"]) >= int(file_log_dropped["value"])

    textlog_total = next(
        (x for x in events if x["event"] == "AsyncLoggingTextLogTotalMessages"), None
    )
    textlog_dropped = next(
        (x for x in events if x["event"] == "AsyncLoggingTextLogDroppedMessages"), None
    )
    assert textlog_total is not None
    assert textlog_dropped is not None
    assert int(textlog_total["value"]) >= int(textlog_dropped["value"])


def test_async_logger_asynchronous_metrics(start_cluster):
    metrics = json.loads(
        node.query(
            """
       SELECT metric, value
       FROM system.asynchronous_metrics
       WHERE name ILIKE '%asynclog%'
       FORMAT JSON
       """
        )
    )["data"]

    # Check that the metric exists
    assert (
        next(
            (x for x in metrics if x["metric"] == "AsyncLoggingErrorFileLogQueueSize"),
            None,
        )
        is not None
    ), metrics
    assert (
        next(
            (x for x in metrics if x["metric"] == "AsyncLoggingFileLogQueueSize"), None
        )
        is not None
    ), metrics
    assert (
        next(
            (x for x in metrics if x["metric"] == "AsyncLoggingConsoleQueueSize"), None
        )
        is not None
    ), metrics
    assert (
        next(
            (x for x in metrics if x["metric"] == "AsyncLoggingTextLogQueueSize"), None
        )
        is not None
    ), metrics
