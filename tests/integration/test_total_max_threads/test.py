import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/config_default.xml"], user_configs=["configs/users.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/config_defined.xml"], user_configs=["configs/users.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_total_max_threads_default(started_cluster):
    node1.query("SELECT count(*) FROM numbers_mt(10000000)", query_id="test_total_max_threads_1");
    node1.query("SYSTEM FLUSH LOGS");
    assert node1.query("select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_1'") == "102\n"


def test_total_max_threads_defined(started_cluster):
    node2.query("SELECT count(*) FROM numbers_mt(10000000)", query_id="test_total_max_threads_2");
    node2.query("SYSTEM FLUSH LOGS");
    assert node2.query("select length(thread_ids) from system.query_log where current_database = currentDatabase() and type = 'QueryFinish' and query_id = 'test_total_max_threads_2'") == "51\n"
