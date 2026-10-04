import time

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/config.xml"
INTROSPECTION_PORT = 9010
FAILPOINT = "database_replicated_startup_pause"
STARTUP_JOB = "startup Replicated database re"
# Every table load and startup job is a dependency of STARTUP_JOB, so those are already done while the
# pause is held. This one depends on all of them, so it is the only job still pending during a pause.
DDL_WORKER_JOB = "startup ddl worker"


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


def introspection_client():
    return Client(node.ip_address, INTROSPECTION_PORT, command=cluster.client_bin_path)


def assert_refused(error, maximum, waiting):
    assert (
        f"Too many simultaneous waiting queries. Maximum: {maximum}, waiting: {waiting}"
        in error
    ), error
    # The message carries the counts, the code is what a caller branches on. Both are published.
    assert "Code: 202." in error, error


def server_setting(name):
    return node.query(
        f"SELECT value FROM system.server_settings WHERE name = '{name}'"
    ).strip()


def waiters_on_startup_job():
    return node.query(
        f"SELECT sum(waiters) FROM system.asynchronous_loader WHERE job = '{STARTUP_JOB}'"
    ).strip()


def waiters_on_ddl_worker_job():
    return node.query(
        f"SELECT sum(waiters) FROM system.asynchronous_loader WHERE job = '{DDL_WORKER_JOB}'"
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
        assert_refused(error, 2, 2)
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
        assert_refused(error, 1, 2)
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

        # The only oracle for the enforcement counter's own decrement: `waiting_queries_amount` is
        # not exposed, and every arm restarts the server, so a counter that leaked its first three
        # waiters is invisible to the metric probe above. Under limit 1 a leak refuses this attach.
        set_config(
            "<max_waiting_queries>0</max_waiting_queries>",
            "<max_waiting_queries>1</max_waiting_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")
        assert server_setting("max_waiting_queries") == "1"

        node.query("DETACH DATABASE re")
        node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
        handles.append(node.get_query_request("ATTACH DATABASE re"))
        wait_for(waiting_queries_metric, "1", "the re-attach to be admitted as the only waiter")
        wait_for(waiters_on_startup_job, "1", "the re-attach to block on the fresh startup job")

        unpin_and_join(handles)
        wait_for(waiting_queries_metric, "0", "the second cycle to drain")
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
        assert_refused(error, 2, 2)
        # Nothing may have been dropped: the refusal happens before the first destructive step.
        assert node.query("EXISTS DATABASE re").strip() == "1"
        assert waiters_on_startup_job() == "2"

        unpin_and_join(handles)
        wait_for(waiting_queries_metric, "0", "every waiter to leave the waiting set")
    finally:
        cleanup(handles)


def test_waiting_queries_limit_covers_ddl_worker_job(started_cluster):
    handles = []
    try:
        pin_startup_of_replicated_database("/test/max_waiting_queries/ddl_worker")
        assert server_setting("max_waiting_queries") == "2"

        for i in range(2):
            handles.append(
                node.get_query_request(
                    f"CREATE TABLE re.w{i} (a Int) ENGINE = MergeTree ORDER BY a"
                )
            )
        wait_for(waiters_on_startup_job, "2", "both queries to block on the startup job")

        # A KILL waits for the ddl worker job in executeDDLQueryOnCluster, and is exempt, so it is
        # admitted past the reached limit and counted. It is submitted before the refusal below, so
        # that refusal reports a waiting count this query is already part of.
        handles.append(
            node.get_query_request(
                "KILL QUERY ON CLUSTER test_shard WHERE query_id = 'no-such-query-id'"
                " SETTINGS distributed_ddl_output_mode = 'none'"
            )
        )
        wait_for(
            waiting_queries_metric, "3", "the exempt kill to be admitted past the limit"
        )
        wait_for(
            waiters_on_ddl_worker_job, "1", "the exempt kill to block on the ddl worker job"
        )

        # A non-exempt query that waits for the same job is refused. Unenforced, it stays blocked on
        # the pinned job, hence the explicit timeout.
        try:
            error = node.query_and_get_error(
                "SELECT * FROM system.distributed_ddl_queue", timeout=60
            )
        except Exception as e:
            raise AssertionError(
                "the query over max_waiting_queries was not refused, it is still waiting"
            ) from e
        assert_refused(error, 2, 3)
        assert waiters_on_ddl_worker_job() == "1"
        assert waiting_queries_metric() == "3"

        # An operator's diagnostic connection has to reach a server that is still loading.
        handles.append(
            introspection_client().get_query_request(
                "SELECT * FROM system.distributed_ddl_queue"
            )
        )
        wait_for(
            waiting_queries_metric,
            "4",
            "the introspection-port query to be admitted past the limit",
        )
        wait_for(
            waiters_on_ddl_worker_job,
            "2",
            "the introspection-port query to block on the ddl worker job",
        )

        # 0 means no limit, so the same query is now admitted and joins the same job's waiters.
        set_config(
            "<max_waiting_queries>2</max_waiting_queries>",
            "<max_waiting_queries>0</max_waiting_queries>",
        )
        node.query("SYSTEM RELOAD CONFIG")
        assert server_setting("max_waiting_queries") == "0"
        handles.append(
            node.get_query_request("SELECT * FROM system.distributed_ddl_queue")
        )
        wait_for(
            waiters_on_ddl_worker_job, "3", "the admitted query to block on the ddl worker job"
        )
        wait_for(waiting_queries_metric, "5", "the admitted query to be counted as waiting")

        unpin_and_join(handles)
        wait_for(waiting_queries_metric, "0", "every waiter to leave the waiting set")
    finally:
        # A failure can land with the limit at 0 or 2, and `set_config` is a `sed` that silently does
        # nothing when its pattern is absent, so restore from every value this test can leave.
        set_config(
            "<max_waiting_queries>0</max_waiting_queries>",
            "<max_waiting_queries>2</max_waiting_queries>",
        )
        cleanup(handles)
