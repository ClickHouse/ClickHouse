import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


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
    node.query("insert into test select 1")
    result = node.query("select * from mv")
    assert int(result) == 1
