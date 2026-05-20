# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clear_workloads_and_resources():
    node.query(
        f"""
        drop workload if exists production;
        drop workload if exists development;
        drop workload if exists admin;
        drop workload if exists vip;
        drop workload if exists all;
        drop resource if exists memory;
    """
    )
    yield


def assert_profile_event(node, query_id, profile_event, check):
    result = node.query(
        f"select ProfileEvents['{profile_event}'] from system.query_log "
        f"where query_id = '{query_id}' "
        f"and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
    ).strip()
    assert result != "", f"Query {query_id} not found in query_log"
    assert check(int(result)), f"Profile event {profile_event} check failed for query {query_id}, got {result}"


def get_current_metric(metric_name):
    return int(node.query(f"select value from system.metrics where metric = '{metric_name}'").strip())


def test_create_workload():
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='1G';
        create workload admin in all settings precedence=-1;
        create workload production in all settings precedence=1, weight=9;
        create workload development in all settings precedence=1, weight=1;
    """
    )

    def do_checks():
        # Check that allocation_queue nodes are created for memory resource
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/admin/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/production/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/development/%' and type='allocation_queue'"
            )
            == "1\n"
        )
        # Check that allocation_limit node is created with max_memory setting
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/all/%' and type='allocation_limit' and resource='memory'"
            )
            == "1\n"
        )

    do_checks()
    node.restart_clickhouse()  # Check that workloads persist
    do_checks()


def test_reserve_memory():
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='10Gi';
        create workload production in all;
        create workload development in all;
    """
    )

    # Run simple queries with workload settings
    node.query(
        "select count(*) from numbers(1000000) settings workload='production', reserve_memory='128Mi'",
        query_id="test_production",
    )
    node.query(
        "select count(*) from numbers(1000000) settings workload='development', reserve_memory='128Mi'",
        query_id="test_development",
    )

    node.query("SYSTEM FLUSH LOGS")

    assert_profile_event(node, "test_production", "MemoryReservationIncreases", lambda x: x == 1)
    assert_profile_event(node, "test_production", "MemoryReservationDecreases", lambda x: x == 1)
    assert_profile_event(node, "test_production", "MemoryReservationKilled", lambda x: x == 0)
    assert_profile_event(node, "test_production", "MemoryReservationFailed", lambda x: x == 0)
    assert_profile_event(node, "test_development", "MemoryReservationIncreases", lambda x: x == 1)
    assert_profile_event(node, "test_development", "MemoryReservationDecreases", lambda x: x == 1)
    assert_profile_event(node, "test_development", "MemoryReservationKilled", lambda x: x == 0)
    assert_profile_event(node, "test_development", "MemoryReservationFailed", lambda x: x == 0)


def test_max_memory_limit():
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='5Mi';
        create workload production in all;
    """
    )

    # This query should fail because it tries to reserve more memory than the workload allows.
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            "select count(*) from numbers(10000000) group by number % 1000000 settings workload='production'",
            query_id="test_max_memory_limit",
        )
    assert "RESOURCE_LIMIT_EXCEEDED" in str(exc_info.value)

    # Test that reserve_memory does not change the outcome.
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            "select count(*) from numbers(10000000) group by number % 1000000 settings workload='production', reserve_memory='1Mi'",
            query_id="test_max_memory_limit",
        )
    assert "RESOURCE_LIMIT_EXCEEDED" in str(exc_info.value)

    # Too high reserve_memory should also fail, not block.
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            "select count(*) from numbers(10000000) group by number % 1000000 settings workload='production', reserve_memory='10Mi'",
            query_id="test_max_memory_limit",
        )
    assert "RESOURCE_LIMIT_EXCEEDED" in str(exc_info.value)

    node.query("SYSTEM FLUSH LOGS")


def test_max_waiting_queries_rejects_extra():
    """Test that when the waiting queue is full, new queries are rejected."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='10Mi';
        create workload production in all settings max_waiting_queries=2;
    """
    )

    errors = []

    def run_blocking_query(query_id):
        try:
            # This query reserves all memory and runs for a while
            # If 3 seconds is not enough, and this is flaky - just remove the whole test, max_waiting_queries is covered by the unittests anyway.
            node.query(
                f"select sleep(3) from numbers(1) "
                f"settings workload='production', reserve_memory='9Mi'",
                query_id=query_id,
            )
        except QueryRuntimeException as e:
            errors.append((query_id, str(e)))

    def run_waiting_query(query_id):
        try:
            # Wait for the blocking query to start
            while (
                node.query(
                    "select count() from system.processes where query_id = 'blocking_query'"
                ).strip()
                == "0"
            ):
                time.sleep(0.1)
            node.query(
                f"select count(*) from numbers(100) settings workload='production', reserve_memory='5Mi'",
                query_id=query_id,
            )
        except QueryRuntimeException as e:
            errors.append((query_id, str(e)))

    # Start one blocking query, then try to start 4 more (2 should wait, 2 should be rejected)
    threads = [threading.Thread(target=run_blocking_query, args=("blocking_query",))]
    for i in range(4):
        threads.append(threading.Thread(target=run_waiting_query, args=(f"waiting_query_{i}",)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert 2 == sum(1 for q, e in errors if "SERVER_OVERLOADED" in e or "MEMORY_RESERVATION_FAILED" in e), f"Expected 2 queries to be rejected. Errors: {errors}"


def test_precedence_kills_lower_priority():
    """Test that higher precedence workload queries can kill lower precedence queries."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='50Mi';
        create workload production in all settings precedence=2;
        create workload vip in all settings precedence=-1;
    """
    )

    results = {"production": None, "vip": None}
    errors = {"production": None, "vip": None}

    def run_production_query():
        try:
            # A production query that reserves most of the memory
            node.query(
                "select sleep(3) from numbers(1) "
                "settings workload='production', reserve_memory='45Mi'",
                query_id="test_production_precedence",
            )
            results["production"] = "success"
        except QueryRuntimeException as e:
            errors["production"] = str(e)
            results["production"] = "killed"

    def run_vip_query():
        try:
            time.sleep(0.3)  # Let production query start first
            # A VIP query with higher precedence that needs memory
            node.query(
                "select count(*) from numbers(1000000) settings workload='vip', reserve_memory='20Mi'",
                query_id="test_vip_precedence",
            )
            results["vip"] = "success"
        except QueryRuntimeException as e:
            errors["vip"] = str(e)
            results["vip"] = "killed"

    t1 = threading.Thread(target=run_production_query)
    t2 = threading.Thread(target=run_vip_query)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results["production"] == "killed" and results["vip"] == "success", \
        f"Expected production to be killed and VIP to succeed. Results: {results}, Errors: {errors}"


def memory_reservation_approved() -> int:
    """Returns the current value of MemoryReservationApproved metric (in bytes)."""
    return int(
        node.query(
            "select value from system.metrics where metric='MemoryReservationApproved'"
        ).strip()
    )


def wait_for_memory_reservation_approved(limit_bytes: int) -> None:
    """
    Wait indefinitely until MemoryReservationApproved reaches limit_bytes,
    asserting along the way that it never exceeds limit_bytes. Relies on
    pytest-timeout to bound the wait if the condition is never met.
    """
    while True:
        approved = memory_reservation_approved()
        assert approved <= limit_bytes, \
            f"MemoryReservationApproved={approved} exceeded limit={limit_bytes}"
        if approved == limit_bytes:
            return


def test_memory_reservation_concurrency():
    """
    Test that memory reservation limits concurrent queries appropriately.
    With 100Mi total limit and 40Mi per query, at most 2 queries can run concurrently.

    Each query holds its reservation by sleeping server-side; we kill all of them
    explicitly once the metric reaches the expected level. This avoids racy timing
    (the queries must outlive the polling) without putting any sleeps in test code.
    """
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='100Mi';
        create workload production in all;
    """
    )

    query_ids = [f"mem_concurrency_query_{i}" for i in range(6)]

    def run_query(query_id):
        try:
            # Server-side sleep — holds the reservation until the query is killed below.
            node.query(
                f"select sleepEachRow(1) from numbers(60) settings max_block_size=1, workload='production', reserve_memory='40Mi'",
                query_id=query_id,
            )
        except QueryRuntimeException:
            pass  # Expected: query was killed

    threads = [threading.Thread(target=run_query, args=(qid,)) for qid in query_ids]
    for t in threads:
        t.start()

    # With 100Mi limit and 40Mi per query, at most 2 can run (80Mi < 100Mi, but 120Mi > 100Mi).
    expected_bytes = 2 * 40 * 1024 * 1024  # 80Mi
    wait_for_memory_reservation_approved(expected_bytes)

    # Release reservations by killing all running queries.
    for qid in query_ids:
        node.query(f"kill query where query_id = '{qid}' sync")

    for t in threads:
        t.join()


def test_cancel_query_with_memory_reservation():
    """
    Test that cancelling a query with an active memory reservation does not cause
    use-after-free. Pipeline threads hold raw pointers to MemoryReservation and call
    syncWithMemoryTracker between processor executions. The pipeline must be stopped
    before the reservation is destroyed.
    """
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='1Gi';
        create workload production in all;
    """
    )

    for i in range(10):
        query_id = f"cancel_reservation_{i}"

        def run_query():
            try:
                # `sleepEachRow(1)` with `max_block_size=1` ticks the pipeline one row
                # per second for up to 60 rows — long enough to be observed and killed,
                # short enough per call to stay under ClickHouse's 3s per-sleep cap.
                node.query(
                    "select sleepEachRow(1) from numbers(60) settings max_block_size=1,"
                    " workload='production', reserve_memory='100Mi'",
                    query_id=query_id,
                )
            except QueryRuntimeException:
                pass  # Expected: query was killed

        t = threading.Thread(target=run_query)
        t.start()

        # Wait indefinitely until the query is registered in system.processes;
        # pytest-timeout will bound this if anything ever goes wrong.
        while (
            node.query(
                f"select count() from system.processes where query_id = '{query_id}'"
            ).strip()
            == "0"
        ):
            pass

        # Cancel — exercises the onCancelOrConnectionLoss / onException path.
        node.query(f"kill query where query_id = '{query_id}' sync")
        t.join()

    # If we got here without sanitizer alerts or crashes, the teardown order is correct.

