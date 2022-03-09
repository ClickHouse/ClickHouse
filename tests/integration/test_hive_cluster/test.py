import logging
import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))




@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance('h0_0_0', main_configs=["configs/cluster.xml"], extra_configs=[ 'configs/hdfs-site.xml'], with_hive=True, with_zookeeper=True)
        cluster.add_instance('h0_0_1', main_configs=["configs/cluster.xml"], extra_configs=[ 'configs/hdfs-site.xml'], with_zookeeper=True)
        cluster.add_instance('h0_1_0', main_configs=["configs/cluster.xml"], extra_configs=[ 'configs/hdfs-site.xml'], with_zookeeper=True)
            
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()

def test_groupby_use_table(started_cluster):
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    CREATE TABLE IF NOT EXISTS default.demo_parquet ON CLUSTER cluster_simple (`id` Nullable(String), `score` Nullable(Int32), `day` String) ENGINE = HiveCluster('cluster_simple', 'thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
    """)
    node.query("set input_format_parquet_allow_missing_columns = true")
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result


def test_groupby_use_function(started_cluster):
    node = started_cluster.instances['h0_0_0']
    node.query("set input_format_parquet_allow_missing_columns = true")
    result = node.query("""
    SELECT day, count(*) FROM hiveCluster('cluster_simple', 'thrift://hivetest:9083', 'tmp', 'demo', 'id Nullable(String), score Nullable(Int32), day String', 'day') group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result