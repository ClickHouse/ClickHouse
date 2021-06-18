#!/usr/bin/env python3

#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
from kazoo.client import KazooClient, KazooState
from kazoo.security import ACL, make_digest_acl, make_acl
from kazoo.exceptions import AuthFailedError, InvalidACLError, NoAuthError, KazooException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', main_configs=['configs/keeper_config.xml', 'configs/logs_conf.xml'], stay_alive=True)

def start_zookeeper():
    node.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh start'])

def stop_zookeeper():
    node.exec_in_container(['bash', '-c', '/opt/zookeeper/bin/zkServer.sh stop'])

def clear_clickhouse_data():
    node.exec_in_container(['bash', '-c', 'rm -fr /var/lib/clickhouse/coordination/logs/* /var/lib/clickhouse/coordination/snapshots/*'])

def convert_zookeeper_data():
    cmd = '/usr/bin/clickhouse keeper-converter --zookeeper-logs-dir /zookeeper/version-2/ --zookeeper-snapshots-dir  /zookeeper/version-2/ --output-dir /var/lib/clickhouse/coordination/snapshots'
    node.exec_in_container(['bash', '-c', cmd])

def stop_clickhouse():
    node.stop_clickhouse()

def start_clickhouse():
    node.start_clickhouse()

def copy_zookeeper_data():
    stop_zookeeper()
    stop_clickhouse()
    clear_clickhouse_data()
    convert_zookeeper_data()
    print(node.exec_in_container)
    start_zookeeper()
    start_clickhouse()

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def get_fake_zk(timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip('node') + ":9181", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance

def get_genuine_zk(timeout=30.0):
    _genuine_zk_instance = KazooClient(hosts=cluster.get_instance_ip('node') + ":2181", timeout=timeout)
    _genuine_zk_instance.start()
    return _genuine_zk_instance

def compare_states(zk1, zk2):

def test_smoke(started_cluster):
    start_zookeeper()

    genuine_connection = get_genuine_zk()
    genuine_connection.create("/test", b"data")

    assert genuine_connection.get("/test")[0] == b"data"

    copy_zookeeper_data()

    fake_connection = get_fake_zk()
    assert fake_connection.get("/test")[0] == b"data"
    assert genuine_connection.get("/test")[0] == b"data"
