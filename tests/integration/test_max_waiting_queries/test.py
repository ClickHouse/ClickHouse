import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/config.xml"
FAILPOINT = "database_replicated_startup_pause"
STARTUP_JOB = "startup Replicated database re"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_config(old, new):
    node.replace_in_config(CONFIG_PATH, old, new)


def pause_failpoint(enabled):
    set_config(
        f"<{FAILPOINT}>{str(not enabled).lower()}</{FAILPOINT}>",
        f"<{FAILPOINT}>{str(enabled).lower()}</{FAILPOINT}>",
    )


def server_setting(name):
    return node.query(
        f"SELECT value FROM system.server_settings WHERE name = '{name}'"
    ).strip()


def waiters_on_startup_job():
    return node.query(
        f"SELECT sum(waiters) FROM system.asynchronous_loader WHERE job = '{STARTUP_JOB}'"
    ).strip()


def waiting_queries_metric():
    return node.query(
        "SELECT value FROM system.metrics WHERE metric = 'WaitingQuery'"
    ).strip()


def wait_for(probe, expected, description, timeout=90):
    """Poll `probe` until it returns `expected`. Every state this waits for is observable in a system
    table, and the paused load job keeps it from changing behind our back, so there are no sleeps."""
    deadline = time.monotonic() + timeout
    observed = None
    while time.monotonic() < deadline:
        observed = probe()
        if observed == expected:
            return
        time.sleep(0.1)
    raise AssertionError(
        f"Timed out waiting for {description}: expected {expected!r}, last saw {observed!r}"
    )


def pin_startup_of_replicated_database(zk_path):
    """Leave `re`'s startup load job blocked inside the pause fail point, so queries that wait for
    the database to start pile up in a set that only this test adds to."""
    node.query("DROP DATABASE IF EXISTS re SYNC")
    node.query(f"CREATE DATABASE re ENGINE = Replicated('{zk_path}', 's1', 'r1')")
    node.query("CREATE TABLE re.t (a Int) ENGINE = MergeTree ORDER BY a")

    pause_failpoint(True)
    node.restart_clickhouse()
    node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE")


def unpin_and_join(handles):
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
    for handle in handles:
        _, error = handle.get_answer_and_error()
        assert error == "", error
    handles.clear()


def cleanup(handles):
    # Put the settings back before draining anything. A query that resumes under a limit the test
    # lowered can block for its whole client timeout, and this runs from `finally`, where that would
    # be reported as a session timeout instead of the assertion that actually failed.
    node.query("SYSTEM RELOAD CONFIG", ignore_error=True)
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}", ignore_error=True)
    for handle in handles:
        try:
            handle.get_answer_and_error()
        except Exception:
            pass
    pause_failpoint(False)
    node.query("DROP DATABASE IF EXISTS re SYNC", ignore_error=True)


def test_waiting_queries_limit(started_cluster):
    handles = []
    try:
        pin_startup_of_replicated_database("/test/max_waiting_queries/limit")
        assert server_setting("max_waiting_queries") == "2"

        # Each of these blocks in DatabaseReplicated::waitDatabaseStarted().
        for i in range(2):
            handles.append(
                node.get_query_request(
                    f"CREATE TABLE re.w{i} (a Int) ENGINE = MergeTree ORDER BY a"
                )
            )
        wait_for(waiters_on_startup_job, "2", "both queries to block on the startup job")

        # The limit is reached, so the next query is refused instead of joining the waiters. An
        # unenforced limit leaves it blocked on the pinned job, hence the explicit timeout.
        try:
            error = node.query_and_get_error(
                "CREATE TABLE re.refused (a Int) ENGINE = MergeTree ORDER BY a", timeout=60
            )
        except Exception as e:
            raise AssertionError(
                "the query over max_waiting_queries was not refused, it is still waiting"
            ) from e
        assert "Too many simultaneous waiting queries" in error, error
        assert waiters_on_startup_job() == "2"
        assert waiting_queries_metric() == "2"

        # 0 means no limit, so a query that would have been refused above is now admitted. A server
        # left on the default value must never refuse a query for waiting.
        set_config(
            "<max_waiting_queries>2</max_waiting_queries>",
            "<max_waiting_queries>0</max_waiting_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")
        assert server_setting("max_waiting_queries") == "0"
        handles.append(
            node.get_query_request(
                "CREATE TABLE re.admitted (a Int) ENGINE = MergeTree ORDER BY a"
            )
        )
        wait_for(waiters_on_startup_job, "3", "the third query to block on the startup job")
        assert waiting_queries_metric() == "3"

        unpin_and_join(handles)
        wait_for(waiting_queries_metric, "0", "every waiter to leave the waiting set")
    finally:
        set_config(
            "<max_waiting_queries>0</max_waiting_queries>",
            "<max_waiting_queries>2</max_waiting_queries>",
        )
        cleanup(handles)


def test_waiting_queries_do_not_hold_concurrency_slots(started_cluster):
    handles = []
    try:
        pin_startup_of_replicated_database("/test/max_waiting_queries/discount")
        assert server_setting("max_concurrent_queries") == "0"

        handles.append(
            node.get_query_request(
                "CREATE TABLE re.w (a Int) ENGINE = MergeTree ORDER BY a"
            )
        )
        wait_for(waiters_on_startup_job, "1", "the query to block on the startup job")

        # Lower max_concurrent_queries to exactly the number of queries in the process list. Both the
        # reload and every query after it run while that one query is waiting; the reload itself is
        # admitted because the limit it installs is not in effect yet when it starts.
        set_config(
            "<max_concurrent_queries>0</max_concurrent_queries>",
            "<max_concurrent_queries>1</max_concurrent_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")

        # max_waiting_queries documents that waiting queries are not counted against the
        # max_concurrent_* limits. Without that, the waiter holds the only slot and this is refused
        # with "Too many simultaneous queries. Maximum: 1".
        assert node.query("SELECT 1", settings={"queue_max_wait_ms": 0}).strip() == "1"
        # Proves the reload took effect, so the query above was not admitted vacuously.
        assert server_setting("max_concurrent_queries") == "1"

        # Lift the limit before releasing the waiter: the Replicated DDL worker runs the statement
        # again as a nested query, so the resumed CREATE TABLE needs a second process list slot.
        set_config(
            "<max_concurrent_queries>1</max_concurrent_queries>",
            "<max_concurrent_queries>0</max_concurrent_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")

        unpin_and_join(handles)
    finally:
        set_config(
            "<max_concurrent_queries>1</max_concurrent_queries>",
            "<max_concurrent_queries>0</max_concurrent_queries>",
        )
        cleanup(handles)
