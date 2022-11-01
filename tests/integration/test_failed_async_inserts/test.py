import logging
from time import sleep

import pytest
from helpers.cluster import ClickHouseCluster


from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()



def test_failed_async_inserts(
    started_cluster
):
    node = started_cluster.instances["node"]

    node.query("CREATE TABLE async_insert_30_10_2022 (id UInt32, s String) ENGINE = Memory")

    try:
        node.query("SET async_insert = 1")
        node.query("INSERT INTO async_insert_30_10_2022 VALUES ()")
        node.query("INSERT INTO async_insert_30_10_2022 VALUES ([1,2,3], 1)")
        node.query('INSERT INTO async_insert_30_10_2022 format JSONEachRow {"id" : 1} {"x"}')
        node.query("INSERT INTO async_insert_30_10_2022 Values (throwIf(4),'')")
    except Exception:
      return None


    select_query = "SELECT value FROM system.events WHERE event == 'FailedAsyncInsertQuery'"
    
    assert node.query(select_query) == "4\n"
    
    node.query("DROP TABLE IF EXISTS async_insert_30_10_2022 NO DELAY")