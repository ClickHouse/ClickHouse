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
        lambda x: x == 100,
    )
    assert_profile_event(
        node1,
        "test_concurrent_threads_soft_limit_1",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 0,
    )
    assert (
        node1.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_1' order by query_start_time_microseconds desc limit 1"
        )
        == "102\n"
    )


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
        lambda x: x == 50,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x == 50,
    )
    assert_profile_event(
        node2,
        "test_concurrent_threads_soft_limit_2",
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 1,
    )
    assert (
        node2.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_2' order by query_start_time_microseconds desc limit 1"
        )
        == "52\n"
    )


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
        lambda x: x == 99,
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
        "ConcurrencyControlQueriesDelayed",
        lambda x: x == 1,
    )
    assert (
        node3.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_concurrent_threads_soft_limit_3' order by query_start_time_microseconds desc limit 1"
        )
        == "3\n"
    )


# In config_limit_reached.xml there is concurrent_threads_soft_limit=10
# Background query starts in a separate thread to reach this limit.
# When this limit is reached the foreground query gets less than 5 queries despite the fact that it has settings max_threads=5
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
        return count >= 10

    while not limit_reached():
        time.sleep(0.1)

    node4.query(
        "SELECT count(*) FROM numbers_mt(10000000) settings max_threads=5",
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
    assert count < 5
    node4.query("KILL QUERY WHERE query_id = 'background_query' SYNC")
    background_thread.join()
