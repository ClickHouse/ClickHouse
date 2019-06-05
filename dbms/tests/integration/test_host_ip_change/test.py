import time
import pytest

import subprocess
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml', 'configs/listen_host.xml'], with_zookeeper=True, ipv6_address='2001:3984:3989::1:1111')
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml', 'configs/listen_host.xml'], with_zookeeper=True, ipv6_address='2001:3984:3989::1:1112')


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query(
                '''
                CREATE DATABASE IF NOT EXISTS test;
                CREATE TABLE IF NOT EXISTS test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', '{}')
                ORDER BY id PARTITION BY toYYYYMM(date);
                '''.format(node.name)
            )

        yield cluster

    except Exception as ex:
        print ex

    finally:
        cluster.shutdown()
        pass


def test_merge_doesnt_work_without_zookeeper(start_cluster):
    # First we check, that normal replication works
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)")
    assert node1.query("SELECT count(*) from test_table") == "3\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "3")

    # We change source node ip
    cluster.restart_instance_with_ip_change(node1, "2001:3984:3989::1:7777")

    # Put some data to source node1
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)")
    # Check that data is placed on node1
    assert node1.query("SELECT count(*) from test_table") == "6\n"

    # Because of DNS cache dest node2 cannot download data from node1
    with pytest.raises(Exception):
        assert_eq_with_retry(node2, "SELECT count(*) from test_table", "6")

    # drop DNS cache
    node2.query("SYSTEM DROP DNS CACHE")
    # Data is downloaded
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "6")

    # Just to be sure check one more time
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 8)")
    assert node1.query("SELECT count(*) from test_table") == "7\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "7")
