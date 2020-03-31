import time
import pytest
import os

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', config_dir="configs")

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_system_tables_non_lazy_load(start_cluster):
    assert node1.query_and_get_error("SELECT * FROM system.part_log") == ""
    assert node1.query_and_get_error("SELECT * FROM system.query_log") == ""
    assert node1.query_and_get_error("SELECT * FROM system.query_thread_log") == ""
    assert node1.query_and_get_error("SELECT * FROM system.text_log") == ""
    assert node1.query_and_get_error("SELECT * FROM system.trace_log") == ""
    assert node1.query_and_get_error("SELECT * FROM system.metric_log") == ""
