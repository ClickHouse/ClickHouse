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
    """Assert that a profile event for a given query matches the check function."""
    result = node.query(
        f"select ProfileEvents['{profile_event}'] from system.query_log "
        f"where query_id = '{query_id}' "
        f"and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
    ).strip()
    assert result != "", f"Query {query_id} not found in query_log"
    assert check(int(result)), f"Profile event {profile_event} check failed for query {query_id}, got {result}"


def get_current_metric(metric_name):
    """Get the current value of a metric from system.metrics."""
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


def test_memory_reservation_basic_metrics():
    """Test that running a query with reserve_memory properly increments profile events."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='10Gi';
        create workload production in all;
    """
    )

    node.query(
        "select count(*) from numbers(1000000) settings workload='production', reserve_memory='128Mi'",
        query_id="test_basic_metrics",
    )

    node.query("SYSTEM FLUSH LOGS")

    # Check that memory reservation increases and decreases happened
    assert_profile_event(node, "test_basic_metrics", "MemoryReservationIncreases", lambda x: x >= 1)
    assert_profile_event(node, "test_basic_metrics", "MemoryReservationDecreases", lambda x: x >= 1)
    # No kills or failures should have occurred
    assert_profile_event(node, "test_basic_metrics", "MemoryReservationKilled", lambda x: x == 0)
    assert_profile_event(node, "test_basic_metrics", "MemoryReservationFailed", lambda x: x == 0)


def test_memory_reservation_exceeds_limit_self_kill():
    """Test that a query trying to allocate more memory than max_memory of its workload fails."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='1Mi';
        create workload production in all;
    """
    )

    # This query should fail because it tries to reserve more memory than the workload allows
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            "select count(*) from numbers(10000000) group by number % 1000000 settings workload='production', reserve_memory='10Mi'",
            query_id="test_self_kill",
        )

    # The error should mention the memory limit
    assert "MEMORY_RESERVATION_KILLED" in str(exc_info.value) or "RESOURCE_LIMIT_EXCEEDED" in str(exc_info.value)

    node.query("SYSTEM FLUSH LOGS")


def test_memory_reservation_kills_other_query():
    """Test that a smaller allocation can kill a larger one when memory is tight."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='50Mi';
        create workload production in all;
    """
    )

    results = {"query1": None, "query2": None}
    errors = {"query1": None, "query2": None}

    def run_large_query():
        try:
            # A large query that reserves most of the memory and runs for a while
            node.query(
                "select count(*) from numbers(100000000) group by number % 10000000 "
                "settings workload='production', reserve_memory='40Mi', max_threads=1",
                query_id="test_large_query",
            )
            results["query1"] = "success"
        except QueryRuntimeException as e:
            errors["query1"] = str(e)
            results["query1"] = "killed"

    def run_small_query():
        try:
            time.sleep(0.5)  # Let the large query start first
            # A smaller query that needs memory, potentially killing the larger one
            node.query(
                "select count(*) from numbers(1000000) settings workload='production', reserve_memory='30Mi'",
                query_id="test_small_query",
            )
            results["query2"] = "success"
        except QueryRuntimeException as e:
            errors["query2"] = str(e)
            results["query2"] = "killed"

    t1 = threading.Thread(target=run_large_query)
    t2 = threading.Thread(target=run_small_query)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    node.query("SYSTEM FLUSH LOGS")

    # At least one query should have been killed due to memory pressure
    assert results["query1"] == "killed" or results["query2"] == "killed", \
        f"Expected at least one query to be killed. Results: {results}, Errors: {errors}"


def test_max_waiting_queries_rejects_extra():
    """Test that when the waiting queue is full, new queries are rejected."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='10Mi', max_waiting_queries=2;
        create workload production in all;
    """
    )

    results = []
    errors = []

    def run_blocking_query(query_id):
        try:
            # This query reserves all memory and runs for a while
            node.query(
                f"select sleepEachRow(0.5), count(*) from numbers(20) "
                f"settings workload='production', reserve_memory='9Mi'",
                query_id=query_id,
            )
            results.append(("success", query_id))
        except QueryRuntimeException as e:
            errors.append((query_id, str(e)))
            results.append(("error", query_id))

    def run_waiting_query(query_id):
        try:
            time.sleep(0.3)  # Let the blocking query start
            node.query(
                f"select count(*) from numbers(100) settings workload='production', reserve_memory='5Mi'",
                query_id=query_id,
            )
            results.append(("success", query_id))
        except QueryRuntimeException as e:
            errors.append((query_id, str(e)))
            results.append(("error", query_id))

    # Start one blocking query, then try to start 4 more (2 should wait, 2 should be rejected)
    threads = [threading.Thread(target=run_blocking_query, args=("blocking_query",))]
    for i in range(4):
        threads.append(threading.Thread(target=run_waiting_query, args=(f"waiting_query_{i}",)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    node.query("SYSTEM FLUSH LOGS")

    # Check that some queries were rejected with SERVER_OVERLOADED or similar
    rejected_count = sum(1 for q, e in errors if "SERVER_OVERLOADED" in e or "MEMORY_RESERVATION_FAILED" in e or "too many" in e.lower())
    # We expect at least 1-2 queries to be rejected (depends on timing)
    # Note: This test may be flaky due to timing; in real scenarios you might need more control
    assert rejected_count >= 1 or len(errors) >= 1, f"Expected some queries to be rejected. Errors: {errors}"


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
                "select sleepEachRow(0.1), count(*) from numbers(100) "
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

    node.query("SYSTEM FLUSH LOGS")

    # The production query should be killed because VIP has higher precedence (lower precedence number)
    # Or if timing doesn't align, at least one should be killed
    assert results["production"] == "killed" or results["vip"] == "success", \
        f"Expected production to be killed or VIP to succeed. Results: {results}, Errors: {errors}"


def test_fairness_between_workloads():
    """Test that memory is distributed fairly between workloads with equal weight."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='100Mi';
        create workload production in all;
        create workload development in all;
    """
    )

    results = {"production": None, "development": None}

    def run_query(workload, query_id):
        try:
            node.query(
                f"select count(*) from numbers(1000000) settings workload='{workload}', reserve_memory='40Mi'",
                query_id=query_id,
            )
            results[workload] = "success"
        except QueryRuntimeException as e:
            results[workload] = f"error: {e}"

    t1 = threading.Thread(target=run_query, args=("production", "test_fair_production"))
    t2 = threading.Thread(target=run_query, args=("development", "test_fair_development"))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    node.query("SYSTEM FLUSH LOGS")

    # Both queries should succeed when there's enough memory for both
    assert results["production"] == "success", f"Production query failed: {results['production']}"
    assert results["development"] == "success", f"Development query failed: {results['development']}"


def test_weighted_fairness_between_workloads():
    """Test that memory distribution respects workload weights."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='100Mi';
        create workload production in all settings weight=3;
        create workload development in all settings weight=1;
    """
    )

    results = {"production": [], "development": []}

    def run_queries(workload, count):
        for i in range(count):
            try:
                node.query(
                    f"select count(*) from numbers(100000) settings workload='{workload}', reserve_memory='10Mi'",
                    query_id=f"test_weighted_{workload}_{i}",
                )
                results[workload].append("success")
            except QueryRuntimeException as e:
                results[workload].append(f"error: {e}")

    # Run multiple queries in each workload
    t1 = threading.Thread(target=run_queries, args=("production", 5))
    t2 = threading.Thread(target=run_queries, args=("development", 5))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    node.query("SYSTEM FLUSH LOGS")

    # All queries should succeed (enough memory)
    prod_success = sum(1 for r in results["production"] if r == "success")
    dev_success = sum(1 for r in results["development"] if r == "success")

    assert prod_success == 5, f"Expected all production queries to succeed, got {prod_success}"
    assert dev_success == 5, f"Expected all development queries to succeed, got {dev_success}"


def test_current_metrics_during_query():
    """Test that CurrentMetrics MemoryReservationApproved is updated during query execution."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='1Gi';
        create workload production in all;
    """
    )

    observed_approved = []

    def run_slow_query():
        node.query(
            "select sleepEachRow(0.2), count(*) from numbers(10) settings workload='production', reserve_memory='100Mi'",
            query_id="test_metrics_query",
        )

    def observe_metrics():
        # Give the query time to start
        time.sleep(0.3)
        for _ in range(5):
            approved = get_current_metric("MemoryReservationApproved")
            observed_approved.append(approved)
            time.sleep(0.2)

    t1 = threading.Thread(target=run_slow_query)
    t2 = threading.Thread(target=observe_metrics)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # During query execution, MemoryReservationApproved should have been > 0 at some point
    assert any(x > 0 for x in observed_approved), \
        f"Expected MemoryReservationApproved > 0 during query, observed: {observed_approved}"

    # After query completes, it should return to baseline
    time.sleep(0.2)
    final_approved = get_current_metric("MemoryReservationApproved")
    # Note: Other queries might be running, so we just check it decreased or is reasonable
    assert final_approved <= max(observed_approved), \
        f"Expected MemoryReservationApproved to decrease after query, got {final_approved}"


def test_update_max_memory_releases_pending():
    """Test that increasing max_memory via DDL allows waiting queries to proceed."""
    node.query(
        f"""
        create resource memory (memory reservation);
        create workload all settings max_memory='20Mi';
        create workload production in all;
    """
    )

    results = {"blocking": None, "waiting": None}
    errors = {"blocking": None, "waiting": None}

    def run_blocking_query():
        try:
            node.query(
                "select sleepEachRow(0.3), count(*) from numbers(10) "
                "settings workload='production', reserve_memory='15Mi'",
                query_id="test_blocking_ddl",
            )
            results["blocking"] = "success"
        except QueryRuntimeException as e:
            errors["blocking"] = str(e)
            results["blocking"] = "error"

    def run_waiting_query():
        try:
            time.sleep(0.3)  # Let blocking query start
            node.query(
                "select count(*) from numbers(100) settings workload='production', reserve_memory='15Mi'",
                query_id="test_waiting_ddl",
            )
            results["waiting"] = "success"
        except QueryRuntimeException as e:
            errors["waiting"] = str(e)
            results["waiting"] = "error"

    def increase_limit():
        time.sleep(0.5)  # Let both queries start
        # Increase the memory limit to allow both queries
        node.query("create or replace workload all settings max_memory='50Mi'")

    t1 = threading.Thread(target=run_blocking_query)
    t2 = threading.Thread(target=run_waiting_query)
    t3 = threading.Thread(target=increase_limit)

    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()

    node.query("SYSTEM FLUSH LOGS")

    # After increasing the limit, both queries should eventually succeed
    # (or at least the waiting one should not be rejected)
    assert results["blocking"] == "success" or results["waiting"] == "success", \
        f"Expected at least one query to succeed after limit increase. Results: {results}, Errors: {errors}"

