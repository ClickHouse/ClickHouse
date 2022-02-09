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
        cluster.add_instance('h0_0_0', main_configs=["configs/cluster.xml"], main_configs=['configs/config.xml'], extra_configs=[ 'configs/hdfs-site.xml'], with_hive=True)
        cluster.add_instance('h0_0_1', main_configs=["configs/cluster.xml"], main_configs=['configs/config.xml'], extra_configs=[ 'configs/hdfs-site.xml'], with_hive=True)
        cluster.add_instance('h0_1_0', main_configs=["configs/cluster.xml"], main_configs=['configs/config.xml'], extra_configs=[ 'configs/hdfs-site.xml'], with_hive=True)
            
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()

def test_create_parquet_table(started_cluster):
    node = started_cluster.instances['h0_0_0']
    node.query("set input_format_parquet_allow_missing_columns = true")
    result = node.query("""
    DROP TABLE IF EXISTS default.demo_parquet 
    """)
    result = node.query("""
    CREATE TABLE default.demo_parquet (`id` Nullable(String), `score` Nullable(Int32), `day` String) ENGINE = HiveCluster('cluster_simple', 'thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
    """)
    assert result.strip() == ''

def test_groupby_use_table(started_cluster):
    node = started_cluster.instances['h0_0_0']
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
    SELECT day, count(*) FROM HiveCluster('cluster_simple', 'thrift://hivetest:9083', 'tmp', 'demo', 'id Nullable(String), score Nullable(Int32), day String', 'day') group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result