import os
import sys
import time
from multiprocessing.dummy import Pool
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query(
            'create table t (number UInt64) engine = Distributed(test_cluster_two_shards, system, numbers);'
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_filled_async_drain_connection_pool(started_cluster):
    busy_pool = Pool(10)

    def execute_query(i):
        for _ in range(100):
            node.query('select * from t where number = 0 limit 2;',
                       settings={
                           "sleep_in_receive_cancel_ms": 10000000,
                           "max_execution_time": 5
                       })

    p = busy_pool.map(execute_query, range(10))
