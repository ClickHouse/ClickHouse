import pytest
import time
import logging

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=["configs/async_load_system_database.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_materialized_view_with_subquery(start_cluster):
    node.query("create table test (x UInt32) engine=TinyLog()")
    node.query(
        "create materialized view mv engine = TinyLog() as with subquery as (select * from test) select * from subquery"
    )
    node.restart_clickhouse(kill=True)
    time.sleep(5)
    dependencies_table=node.query("select dependencies_table from system.tables where name='test'")
    logging.debug(f"dependencies_table {dependencies_table}")
    node.query("insert into test select 1")
    result = node.query("select * from mv")
    assert int(result) == 1
