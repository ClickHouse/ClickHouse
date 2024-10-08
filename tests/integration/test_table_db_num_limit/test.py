import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["config/config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_table_db_limit(started_cluster):
    # By the way, default database already exists.
    for i in range(9):
        node.query("create database db{}".format(i))

    with pytest.raises(QueryRuntimeException) as exp_info:
        node.query("create database db_exp".format(i))

    assert "TOO_MANY_DATABASES" in str(exp_info)

    for i in range(10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    # This checks that system tables are not accounted in the number of tables.
    node.query("system flush logs")

    for i in range(10):
        node.query("drop table t{}".format(i))

    for i in range(10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    with pytest.raises(QueryRuntimeException) as exp_info:
        node.query("create table default.tx (a Int32) Engine = Log")

    assert "TOO_MANY_TABLES" in str(exp_info)
