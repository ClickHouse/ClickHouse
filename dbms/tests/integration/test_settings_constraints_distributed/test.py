import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_on_cluster.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_on_cluster.xml'])

distributed = cluster.add_instance('distributed', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_on_distributed.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query("CREATE TABLE sometable(date Date, id UInt32, value Int32) ENGINE = MergeTree() ORDER BY id;")
            node.query("INSERT INTO sometable VALUES (toDate('2020-01-20'), 1, 1)")

        distributed.query("CREATE TABLE proxy (date Date, id UInt32, value Int32) ENGINE = Distributed(test_cluster, default, sometable);")

        yield cluster

    finally:
        cluster.shutdown()

def test_settings_under_remote(started_cluster):
    assert distributed.query("SELECT COUNT() FROM proxy") == '1\n'

    with pytest.raises(QueryRuntimeException):
        distributed.query("SELECT COUNT() FROM proxy", user='remote')

    assert distributed.query("SELECT COUNT() FROM proxy", settings={"max_memory_usage": 1000000}, user='remote') == '1\n'

    with pytest.raises(QueryRuntimeException):
        distributed.query("SELECT COUNT() FROM proxy", settings={"max_memory_usage": 1000001}, user='remote')
