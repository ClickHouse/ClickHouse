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
        cluster.add_instance('h0_0_0', main_configs=['configs/config.xml'], other_configs=[ 'configs/hdfs-site.xml'], with_hive=True)
        
        logging.info("Starting cluster ...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_create_parquet_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    CREATE TABLE default.demo_parquet (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
            """)
    logging.info("create result {}".format(result))
 
    assert result.strip() == ''

def test_create_orc_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    CREATE TABLE default.demo_orc (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_orc') PARTITION BY(day)
            """)
    logging.info("create result {}".format(result))
    
    assert result.strip() == ''

def test_create_text_table(started_cluster):
    logging.info('Start testing creating hive table ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    CREATE TABLE default.demo_text (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String)) ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo_text') PARTITION BY (tuple())
            """)
    logging.info("create result {}".format(result))
    
    assert result.strip() == ''

def test_parquet_groupby(started_cluster):
    logging.info('Start testing groupby ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_parquet group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result
def test_orc_groupby(started_cluster):
    logging.info('Start testing groupby ...')
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_orc group by day order by day
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result

def test_text_count(started_cluster):
    node = started_cluster.instances['h0_0_0']
    result = node.query("""
    SELECT day, count(*) FROM default.demo_orc group by day order by day SETTINGS format_csv_delimiter = '\x01'
            """)
    expected_result = """2021-11-01	1
2021-11-05	2
2021-11-11	1
2021-11-16	2
"""
    assert result == expected_result
