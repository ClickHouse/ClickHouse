# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_default",
    main_configs=["configs/config.d/config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_system_logs_order_by_expr(start_cluster):
    node.query("SET log_query_threads = 1")
    node.query("SELECT count() FROM system.tables")
    node.query("SYSTEM FLUSH LOGS")

    # system.query_log
    assert node.query("SELECT sorting_key FROM system.tables WHERE database='system' and name='query_log'")
        == "event_date, event_time, initial_query_id\n"
    # system.query_thread_log
    assert node.query("SELECT sorting_key FROM system.tables WHERE database='system' and name='query_thread_log'")
        == "event_date, event_time, query_id\n"
