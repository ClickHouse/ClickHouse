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
    table, and the paused load job keeps it from changing behind our back, so no wait here stands in
    for a barrier."""
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


def pin_startup_of_replicated_database(zk_path, create_table=True):
    """Leave `re`'s startup load job blocked inside the pause fail point, so queries that wait for
    the database to start pile up in a set that only this test adds to."""
    node.query("DROP DATABASE IF EXISTS re SYNC")
    node.query(f"CREATE DATABASE re ENGINE = Replicated('{zk_path}', 's1', 'r1')")
    if create_table:
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

        # Each of these blocks in DatabaseReplicated::waitDatabaseStarted.
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
        assert "Too many simultaneous waiting queries. Maximum: 2, waiting: 2" in error, error
        assert waiters_on_startup_job() == "2"
        assert waiting_queries_metric() == "2"

        # Lowering a live nonzero limit cannot cancel queries that are already waiting, it only
        # refuses new ones (`ProcessList::setMaxWaitingQueriesAmount`). This is also the only arm
        # where the check runs with the count already above the limit rather than exactly at it.
        set_config(
            "<max_waiting_queries>2</max_waiting_queries>",
            "<max_waiting_queries>1</max_waiting_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")
        assert server_setting("max_waiting_queries") == "1"
        assert waiters_on_startup_job() == "2"
        assert waiting_queries_metric() == "2"

        try:
            error = node.query_and_get_error(
                "CREATE TABLE re.refused_lower (a Int) ENGINE = MergeTree ORDER BY a", timeout=60
            )
        except Exception as e:
            raise AssertionError(
                "the query over the lowered max_waiting_queries was not refused, it is still waiting"
            ) from e
        assert "Too many simultaneous waiting queries. Maximum: 1, waiting: 2" in error, error
        # The refusal throws before any counter moves, so it must leave the waiting set untouched.
        assert waiters_on_startup_job() == "2"
        assert waiting_queries_metric() == "2"

        # 0 means no limit, so a query that would have been refused above is now admitted. A server
        # left on the default value must never refuse a query for waiting.
        set_config(
            "<max_waiting_queries>1</max_waiting_queries>",
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
        # A failure can land with the limit at 0, 1 or 2, and `set_config` is a `sed` that silently
        # does nothing when its pattern is absent, so restore from every value this test can leave.
        for live in ["0", "1"]:
            set_config(
                f"<max_waiting_queries>{live}</max_waiting_queries>",
                "<max_waiting_queries>2</max_waiting_queries>",
            )
        cleanup(handles)


def test_waiting_queries_limit_refuses_database_drop(started_cluster):
    handles = []
    try:
        pin_startup_of_replicated_database(
            "/test/max_waiting_queries/drop", create_table=False
        )
        assert server_setting("max_waiting_queries") == "2"

        for i in range(2):
            handles.append(
                node.get_query_request(
                    f"CREATE TABLE re.w{i} (a Int) ENGINE = MergeTree ORDER BY a"
                )
            )
        wait_for(waiters_on_startup_job, "2", "both queries to block on the startup job")

        # The database has no tables, so nothing between the interpreter's own wait and the catalog
        # removal waits for the startup job. Unrefused, this drop reaches `~LoadTask`, whose cleanup
        # wait cannot be refused, and parks there holding the exclusive database DDL guard, hence the
        # explicit timeout.
        try:
            error = node.query_and_get_error("DROP DATABASE re SYNC", timeout=60)
        except Exception as e:
            raise AssertionError(
                "DROP DATABASE over max_waiting_queries was not refused, it is still waiting"
            ) from e
        assert "Too many simultaneous waiting queries. Maximum: 2, waiting: 2" in error, error
        # Nothing may have been dropped: the refusal happens before the first destructive step.
        assert node.query("EXISTS DATABASE re").strip() == "1"
        assert waiters_on_startup_job() == "2"

        unpin_and_join(handles)
        wait_for(waiting_queries_metric, "0", "every waiter to leave the waiting set")
    finally:
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

        # The same discount is promised for the per-user and all-users limits, which are query-level
        # settings, so each probe carries its own limit instead of reloading the config. One query is
        # waiting, so a limit of 1 is reached unless that query is discounted; selecting the limit
        # back proves the server ran the check against the value this probe set. 1 proves the discount
        # is applied at all; the UInt64 maximum proves the sum saturates instead of wrapping to 0 and
        # refusing everything.
        for limit in ["max_concurrent_queries_for_user", "max_concurrent_queries_for_all_users"]:
            for value in ["1", "18446744073709551615"]:
                assert (
                    node.query(
                        f"SELECT getSetting('{limit}')", settings={limit: value}
                    ).strip()
                    == value
                )

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
        wait_for(waiting_queries_metric, "0", "every waiter to leave the waiting set")

        # Same two limits, same value, but now with nothing waiting: the query the discount admitted
        # above must be refused. That is the negative control for those probes (it passes only if the
        # guards actually run) and the post-drain oracle for the counters (one that was not
        # decremented would discount this query too and admit it). The absent ", waiting:" suffix
        # pins the counter at exactly zero.
        wait_for(
            lambda: node.query(
                "SELECT count() FROM system.processes WHERE query NOT LIKE '%system.processes%'"
            ).strip(),
            "0",
            "the process list to drain",
        )
        occupancy = node.get_query_request(
            "SELECT sleepEachRow(1) FROM numbers(120) SETTINGS "
            "function_sleep_max_microseconds_per_block = 0, max_block_size = 1",
            query_id="occupancy",
        )
        try:
            wait_for(
                lambda: node.query(
                    "SELECT count() FROM system.processes WHERE query_id = 'occupancy'"
                ).strip(),
                "1",
                "the occupancy query to enter the process list",
            )
            for limit, whose in [
                ("max_concurrent_queries_for_user", "for user default"),
                ("max_concurrent_queries_for_all_users", "for all users"),
            ]:
                error = node.query_and_get_error("SELECT 1", settings={limit: 1})
                assert (
                    f"Too many simultaneous queries {whose}. Current: 1, maximum: 1" in error
                ), error
                assert ", waiting:" not in error, error

            # `max_concurrent_queries` is a server setting, so its own negative control installs the
            # limit from the config while the occupancy query already holds the only slot. The reload
            # is admitted because the limit it installs is not in effect yet when it starts, and
            # `Maximum: 1` proves the check ran against the value it installed. Nothing else runs
            # after this: the limit is in effect until the occupancy query is killed below, and only
            # `KILL QUERY` and `system.processes` selects are exempt from it.
            set_config(
                "<max_concurrent_queries>0</max_concurrent_queries>",
                "<max_concurrent_queries>1</max_concurrent_queries>",
            )
            node.query("SYSTEM RELOAD CONFIG")
            error = node.query_and_get_error("SELECT 1", settings={"queue_max_wait_ms": 0})
            assert "Too many simultaneous queries. Maximum: 1" in error, error
            assert ", waiting:" not in error, error
        finally:
            node.query("KILL QUERY WHERE query_id = 'occupancy' SYNC", ignore_error=True)
            occupancy.get_answer_and_error()
    finally:
        set_config(
            "<max_concurrent_queries>1</max_concurrent_queries>",
            "<max_concurrent_queries>0</max_concurrent_queries>",
        )
        cleanup(handles)
