import threading
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config_default.xml"],
    user_configs=["configs/users.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_defined_50.xml"],
    user_configs=["configs/users.xml"],
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config_defined_1.xml"],
    user_configs=["configs/users.xml"],
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/config_limit_reached.xml"],
    user_configs=["configs/users.xml"],
    cpu_limit=5
)


def assert_profile_event(node, query_id, profile_event, check):
    assert check(
        int(
            node.query(
                f"select ProfileEvents['{profile_event}'] from system.query_log where current_database = currentDatabase() and query_id = '{query_id}' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
            )
        )
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_concurrent_threads_soft_limit_default(started_cluster):
    node1.query(
        "SELECT count(*) FROM numbers_mt(10000000)",
        query_id="test_concurrent_threads_soft_limit_1",
    )
    node1.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 1,
    )
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x == 0,
    )
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x >= 1 and x <= 100,
    )
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 1,
    )
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 0,
    )
    s_count = node1.query(
        "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_1' order by query_start_time_microseconds desc limit 1"
    ).strip()
    count = int(s_count) if s_count else 0
    assert count >= 3 and count <= 102


def test_use_concurrency_control_default(started_cluster):
    node1.query(
        "SELECT count(*) FROM numbers_mt(10000000) SETTINGS use_concurrency_control = 0",
        query_id="test_use_concurrency_control",
    )

    # Concurrency control is not used, all metrics should be zeros
    node1.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node1,
        "test_use_concurrency_control",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 0,
    )
    assert_profile_event(
        node1,
        "test_use_concurrency_control",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x == 0,
    )
    assert_profile_event(
        node1,
        "test_use_concurrency_control",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x == 0,
    )
    assert_profile_event(
        node1,
        "test_use_concurrency_control",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 0,
    )
    assert_profile_event(
        node1,
        "test_use_concurrency_control",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 0,
    )


def test_concurrent_threads_soft_limit_defined_50(started_cluster):
    node2.query(
        "SELECT count(*) FROM numbers_mt(10000000)",
        query_id="test_concurrent_threads_soft_limit_2",
    )
    node2.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 1,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x >= 49,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x >= 1 and x <= 50,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 1,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 1,
    )
    s_count = node2.query(
        "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_2' order by query_start_time_microseconds desc limit 1"
    ).strip()
    count = int(s_count) if s_count else 0
    assert count >= 3 and count <= 53


def test_use_concurrency_control_soft_limit_defined_50(started_cluster):
    node2.query(
        "SELECT count(*) FROM numbers_mt(10000000) SETTINGS use_concurrency_control = 0",
        query_id="test_use_concurrency_control_2",
    )
    # Concurrency control is not used, all metrics should be zeros
    node2.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node2,
        "test_use_concurrency_control_2",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 0,
    )
    assert_profile_event(
        node2,
        "test_use_concurrency_control_2",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x == 0,
    )
    assert_profile_event(
        node2,
        "test_use_concurrency_control_2",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x == 0,
    )
    assert_profile_event(
        node2,
        "test_use_concurrency_control_2",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 0,
    )
    assert_profile_event(
        node2,
        "test_use_concurrency_control_2",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 0,
    )


def test_concurrent_threads_soft_limit_defined_1(started_cluster):
    node3.query(
        "SELECT count(*) FROM numbers_mt(10000000)",
        query_id="test_concurrent_threads_soft_limit_3",
    )
    node3.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node3,
        "test_concurrent_threads_soft_limit_3",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 1,
    )
    assert_profile_event(
        node3,
        "test_concurrent_threads_soft_limit_3",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x == 98,
    )
    assert_profile_event(
        node3,
        "test_concurrent_threads_soft_limit_3",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x == 1,
    )
    assert_profile_event(
        node3,
        "test_concurrent_threads_soft_limit_3",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 1,
    )
    assert_profile_event(
        node3,
        "test_concurrent_threads_soft_limit_3",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 1,
    )
    assert (
        node3.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_3' order by query_start_time_microseconds desc limit 1"
        )
        == "4\n"
    )


# In config_limit_reached.xml there is concurrent_threads_soft_limit=10
# Background query starts in a separate thread to reach this limit.
# When this limit is reached the foreground query gets less than 6 threads despite the fact that it has settings max_threads=6
def test_concurrent_threads_soft_limit_limit_reached(started_cluster):
    def background_query():
        try:
            node4.query(
                "SELECT count(*) FROM numbers_mt(1e11) settings max_threads=100",
                query_id="background_query",
            )
        except QueryRuntimeException:
            pass

    background_thread = threading.Thread(target=background_query)
    background_thread.start()

    def limit_reached():
        s_count = node4.query(
            "SELECT sum(length(thread_ids)) FROM system.processes"
        ).strip()
        if s_count:
            count = int(s_count)
        else:
            count = 0
        return count >= 11

    while not limit_reached():
        time.sleep(0.1)

    node4.query(
        "SELECT count(*) FROM numbers_mt(10000000) settings max_threads=6",
        query_id="test_concurrent_threads_soft_limit_4",
    )

    node4.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_4",
        "ConcurrencyControlSlotsGranted",
        lambda x: x == 1,
    )
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_4",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x > 0,
    )
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_4",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x < 5,
    )
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_4",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 1,
    )
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_4",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 1,
    )
    s_count = node4.query(
        "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_4' order by query_start_time_microseconds desc limit 1"
    ).strip()
    if s_count:
        count = int(s_count)
    else:
        count = 0
    assert count < 6
    node4.query("KILL QUERY WHERE query_id = 'background_query' SYNC")
    background_thread.join()


# A query that starts while the limit is reached gets only its non-competing slot. When the other
# query finishes, the released slots are granted to it and it must start using them, even if its
# pipeline never pushes new tasks to the executor's global queue (each stream is an independent
# chain whose steps are slow, as in object storage scans). Previously the executor only retried
# slot acquisition when a step pushed new tasks, so such a query kept running on a single thread
# while the granted slots stayed unused and counted against the limit.
def test_concurrent_threads_soft_limit_upscale_after_release(started_cluster):
    def background_query():
        try:
            node4.query(
                "SELECT count(*) FROM numbers_mt(1e11) settings max_threads=100",
                query_id="background_query_upscale",
            )
        except QueryRuntimeException:
            pass

    background_thread = threading.Thread(target=background_query)
    background_thread.start()

    def limit_reached():
        s_count = node4.query(
            "SELECT sum(length(thread_ids)) FROM system.processes"
        ).strip()
        return int(s_count) >= 11 if s_count else False

    while not limit_reached():
        time.sleep(0.1)

    # 8 streams of 5 blocks each; every block sleeps 100 ms in the filter, so the query takes ~4 s on
    # one thread and ~0.5 s on 8 threads. Timings are sleep-bound, so they do not depend on CPU speed.
    # Without the fix the only thing that triggers slot acquisition is the `max_scheduled_local_tasks`
    # valve of the executor, which fires after 128 local steps (~3.2 s here) regardless of when the
    # slots were released.
    finished_at = {}

    def foreground_query():
        node4.query(
            "SELECT count() FROM numbers_mt(2000) WHERE NOT ignore(sleepEachRow(0.002)) "
            "SETTINGS max_threads=8, max_block_size=50",
            query_id="test_concurrent_threads_soft_limit_5",
        )
        finished_at["t"] = time.monotonic()

    foreground_thread = threading.Thread(target=foreground_query)
    foreground_thread.start()

    def foreground_started():
        return (
            node4.query(
                "SELECT count() FROM system.processes WHERE query_id = 'test_concurrent_threads_soft_limit_5'"
            ).strip()
            == "1"
        )

    while not foreground_started():
        time.sleep(0.1)
    time.sleep(0.5)  # let it run on its single thread for a while

    # Release all competing slots while the foreground query still has ~3.5 s of single-threaded work.
    node4.query("KILL QUERY WHERE query_id = 'background_query_upscale' SYNC")
    released_at = time.monotonic()
    background_thread.join()
    foreground_thread.join()

    # With the released slots picked up right away the rest takes ~0.5 s; without the fix ~2.7 s.
    assert finished_at["t"] - released_at < 1.5

    node4.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_5",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 1,
    )
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_5",
        "ConcurrencyControlSlotsDelayed",
        lambda x: x > 0,
    )
    # 7 competing slots are available for 8 streams once the background query is gone (not checked
    # strictly: a slot granted right before the query finishes may legitimately stay unused).
    assert_profile_event(
        node4,
        "test_concurrent_threads_soft_limit_5",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x >= 4,
    )
