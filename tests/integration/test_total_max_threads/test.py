import pytest
from helpers.cluster import ClickHouseCluster

import threading
import time
from helpers.client import QueryRuntimeException

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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_total_max_threads_default(started_cluster):
    node1.query(
        "SELECT count(*) FROM numbers_mt(10000000)", query_id="test_total_max_threads_1"
    )
    node1.query("SYSTEM FLUSH LOGS")
    assert (
        node1.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_1'"
        )
        == "102\n"
    )


def test_total_max_threads_defined_50(started_cluster):
    node2.query(
        "SELECT count(*) FROM numbers_mt(10000000)", query_id="test_total_max_threads_2"
    )
    node2.query("SYSTEM FLUSH LOGS")
    assert (
        node2.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_2'"
        )
        == "52\n"
    )


def test_total_max_threads_defined_1(started_cluster):
    node3.query(
        "SELECT count(*) FROM numbers_mt(10000000)", query_id="test_total_max_threads_3"
    )
    node3.query("SYSTEM FLUSH LOGS")
    assert (
        node3.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_3'"
        )
        == "3\n"
    )


# In config_limit_reached.xml there is total_max_threads=10
# Background query starts in a separate thread to reach this limit.
# When this limit is reached the foreground query gets less than 5 queries despite the fact that it has settings max_threads=5
def test_total_max_threads_limit_reached(started_cluster):
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
        query_id="test_total_max_threads_4",
    )

    node4.query("SYSTEM FLUSH LOGS")
    s_count = node4.query(
        "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_4'"
    ).strip()
    if s_count:
        count = int(s_count)
    else:
        count = 0
    assert count < 5
    node4.query("KILL QUERY WHERE query_id = 'background_query' SYNC")
    background_thread.join()
