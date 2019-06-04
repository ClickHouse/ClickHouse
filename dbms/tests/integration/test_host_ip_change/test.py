import time
import pytest

import subprocess
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
#node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True, ipv4_address='10.5.1.233')
#node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True, ipv4_address='10.5.1.122')

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml', 'configs/listen_host.xml'], with_zookeeper=True, ipv6_address='2001:3984:3989::1:1111')
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml', 'configs/listen_host.xml'], with_zookeeper=True, ipv6_address='2001:3984:3989::1:1112')
#node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
#node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            print "Creating tables", node.name
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


# Test that outdated parts are not removed when they cannot be removed from zookeeper
def test_merge_doesnt_work_without_zookeeper(start_cluster):
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)")
    assert node1.query("SELECT count(*) from test_table") == "3\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "3")

    print "IP before:", cluster.get_instance_ip("node1")
    cluster.restart_instance_with_ip_change(node1, "2001:3984:3989::1:7777")
    print "IP after:", cluster.get_instance_ip("node1")

    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "3")
    node1.query("INSERT INTO test_table VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)")
    assert node1.query("SELECT count(*) from test_table") == "6\n"

    with pytest.raises(Exception):
        assert_eq_with_retry(node2, "SELECT count(*) from test_table", "6")

    node2.query("SYSTEM DROP DNS CACHE")
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "6")
