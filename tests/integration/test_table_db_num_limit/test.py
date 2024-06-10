import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["config/config.xml"], with_zookeeper=True
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def test_table_db_limit(started_cluster):
    for i in range(6):
        node1.query("create database db{}".format(i))

    with pytest.raises(QueryRuntimeException) as exp_info:
        node1.query("create database db_exp".format(i))

    assert "TOO_MANY_DATABASES" in str(exp_info)
    for i in range(10):
        node1.query("create table t{} (a Int32) Engine = Log".format(i))

    with pytest.raises(QueryRuntimeException) as exp_info:
        node1.query("create table default.tx (a Int32) Engine = Log")
    assert "TOO_MANY_TABLES" in str(exp_info)
