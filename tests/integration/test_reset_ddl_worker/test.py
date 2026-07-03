import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_ddl_worker_reset_count(node):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            "SELECT count() FROM system.text_log WHERE (message='Resetting state as requested') AND (logger_name='DDLWorker') AND (level='Information')"
        ).strip()
    )


def assert_ddl_worker_reset_with_retry(node, prev_reset_count: int):
    node.query("SYSTEM FLUSH LOGS")
    node.query_with_retry(
        "SELECT count() FROM system.text_log WHERE (message='Resetting state as requested') AND (logger_name='DDLWorker') AND (level='Information')",
        check_callback=lambda x: int(x.strip()) == prev_reset_count + 1,
    )


def test_reset_ddl_worker(started_cluster):
    prev_reset_count = get_ddl_worker_reset_count(node1)

    node1.query("SYSTEM RESET DDL WORKER")

    assert_ddl_worker_reset_with_retry(node1, prev_reset_count)

def test_reset_ddl_worker_on_cluster(started_cluster):
    prev_reset_count1 = get_ddl_worker_reset_count(node1)
    prev_reset_count2 = get_ddl_worker_reset_count(node2)

    node1.query("SYSTEM RESET DDL WORKER ON CLUSTER 'test_cluster'")

    assert_ddl_worker_reset_with_retry(node1, prev_reset_count1)
    assert_ddl_worker_reset_with_retry(node2, prev_reset_count2)
