#!/usr/bin/env python3

# Regression test for a server-shutdown ordering bug in DatabaseCatalog::shutdownImpl.
#
# A user database shutdown() can throw (e.g. a table flushAndShutdown hitting a ZooKeeper
# timeout). That exception used to escape the user-database loop before shutdown_system_logs()
# ran. shutdown_system_logs() is what joins the SystemLog flush threads, so skipping it left a
# flush thread alive into the window where main() tears down the static ActivePartsLoading
# thread pool. The flush thread then lazily (re)creates its backing MergeTree table ->
# MergeTreeData::loadDataParts -> StaticThreadPool::get() on the already-reset pool, and the
# server aborts with "The MergeTreePartsLoaderThreadPool is not initialized".
#
# The fix wraps each user-database shutdown() in try/catch so shutdown_system_logs() always
# runs (and joins the flush threads) before the static pools are torn down. This test injects a
# throw in one user table's shutdown (the database_catalog_throw_on_table_shutdown failpoint)
# and asserts the server still shuts down cleanly (no abort, no hang).

import pytest

from helpers.cluster import ClickHouseCluster

FAILPOINT = "database_catalog_throw_on_table_shutdown"
NOT_INITIALIZED = "The MergeTreePartsLoaderThreadPool is not initialized"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_logs_shutdown_when_user_table_shutdown_throws(started_cluster):
    # A user table whose shutdown the failpoint will make throw (a non-predefined database).
    node.query("CREATE DATABASE IF NOT EXISTS userdb")
    node.query(
        "CREATE TABLE IF NOT EXISTS userdb.t (a UInt64) ENGINE = MergeTree ORDER BY a"
    )

    # Generate some activity so the system log flush threads are busy (re)creating their
    # backing tables around shutdown time - the threads that race the static pool teardown.
    for i in range(20):
        node.query(f"SELECT {i}")
    node.query("SYSTEM FLUSH LOGS")

    # Start clean so we only scan this shutdown's log lines.
    node.exec_in_container(
        ["bash", "-c", ": > /var/log/clickhouse-server/clickhouse-server.log"]
    )

    # Make a user table's flushAndShutdown throw, exactly the condition that used to skip
    # system-log shutdown (the same effect a table flushAndShutdown hitting a ZooKeeper timeout
    # would have).
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    # Graceful shutdown - same Context::shutdown / DatabaseCatalog::shutdown path SIGTERM hits.
    # Without the fix the escaped exception leaves the system log flush threads running, so this
    # also guards the "hang" manifestation: stop_clickhouse would time out waiting for exit.
    node.stop_clickhouse(kill=False, stop_wait_sec=60)

    # The injected fault must have fired (otherwise the test proves nothing).
    fault_log = node.grep_in_log(
        "Injecting fault while shutting down table", only_latest=True
    )
    assert fault_log, "Expected the injected table shutdown fault in the log."

    # With the fix, the catalog catches the failed database shutdown and still runs system-log
    # shutdown, so the flush threads are joined on the throwing path.
    system_logs_shutdown = node.grep_in_log("Shutting down system logs", only_latest=True)
    assert system_logs_shutdown, (
        "System logs were not shut down after a user-database shutdown threw. "
        "Their flush threads can then race the static thread pool teardown."
    )

    # And the server must not have aborted with the static-pool-not-initialized LOGICAL_ERROR.
    not_initialized = node.grep_in_log(NOT_INITIALIZED, only_latest=True)
    assert not not_initialized, (
        f"Server hit '{NOT_INITIALIZED}' during shutdown - a SystemLog flush thread "
        "outlived the ActivePartsLoading thread pool teardown."
    )

    # Shutdown must reach the end. This is logged right before the static thread pools are torn
    # down, so its presence means we got past the database/system-log shutdown without aborting.
    assert node.contains_in_log(
        "Background threads finished", from_host=True
    ), "Server shutdown did not complete after a user-database shutdown threw."

    # The leftover storage of the database whose shutdown threw must be released during shutdown,
    # not at process exit. Otherwise it is destroyed by ~DatabaseCatalog at atexit, after the Poco
    # logger registry is gone, which aborts with a LoggerDeleter assertion on stderr.
    aborted = node.grep_in_log("LoggerDeleter", from_host=True, filename="stderr.log")
    assert not aborted, (
        "Server aborted at exit (Poco LoggerDeleter assertion): a storage of the failed database "
        "outlived the logger registry. Its references must be dropped during shutdown."
    )

    # Disable the failpoint and bring the server back up for the next test / teardown.
    node.start_clickhouse()
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
