import pytest
from helpers.cluster import ClickHouseCluster

import threading
import time
import logging

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
        == "51\n"
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
        == "2\n"
    )


def test_total_max_threads_limit_reached(started_cluster):
    logging.debug("ADQM: test begin")

    def thread_select():
        logging.debug("ADQM: started another thread")
        node4.query(
            "SELECT count(*) FROM numbers_mt(1e11) settings max_threads=100",
            query_id="background_query",
        )
        logging.debug("ADQM: finished another thread")

    another_thread = threading.Thread(target=thread_select)
    another_thread.start()

    while (
        node4.query(
            "SELECT count(*) FROM system.processes where query_id = 'background_query'"
        )
        == "0\n"
    ):
        time.sleep(0.1)

    logging.debug("ADQM: started main query")
    node4.query(
        "SELECT count(*) FROM numbers_mt(10000000) settings max_threads=5",
        query_id="test_total_max_threads_4",
    )
    logging.debug("ADQM: finished main query")
    another_thread.join()
    logging.debug("ADQM: logs: %s", node4.grep_in_log("ADQM"))
    node4.query("SYSTEM FLUSH LOGS")
    assert (
        node4.query(
            "select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_4'"
        )
        == "2\n"
    )
    node4.query("KILL QUERY WHERE user = 'default' SYNC")
    logging.debug("ADQM: test end")
