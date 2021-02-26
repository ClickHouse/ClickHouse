import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
NODES = {'node_' + str(i): None for i in (1, 2, 3)}

NODES['node'] = None

sleep_time = 30

@pytest.fixture(scope="module")
def started_cluster():
    NODES['node'] = cluster.add_instance(
    'node', with_zookeeper=True, stay_alive=True, main_configs=['configs/remote_servers.xml'], user_configs=['configs/users.xml'])

    for name in NODES:
        if name != 'node':
            NODES[name] = cluster.add_instance(name, with_zookeeper=True,  user_configs=['configs/users1.xml'])

    try:
        cluster.start()

        for node_id, node in list(NODES.items()):
            node.query('''CREATE TABLE replicated (id UInt32, date Date) ENGINE =
            ReplicatedMergeTree('/clickhouse/tables/replicated', '{}')  ORDER BY id PARTITION BY toYYYYMM(date)'''.format(node_id))

        NODES['node'].query('''CREATE TABLE distributed (id UInt32, date Date) ENGINE =
            Distributed('test_cluster', 'default', 'replicated')''')

        NODES['node'].query("INSERT INTO distributed VALUES (1, '2020-01-01')")

        yield cluster

    finally:
        cluster.shutdown()


config = '''<yandex>
    <profiles>
        <default>
            <sleep_in_send_tables_status>{sleep_in_send_tables_status}</sleep_in_send_tables_status>
            <sleep_in_send_data>{sleep_in_send_data}</sleep_in_send_data>
        </default>
    </profiles>
</yandex>'''


def check_query(expected_replica, receive_timeout=300):
    NODES['node'].restart_clickhouse()
    
    # Without hedged requests select query will last more than 30 seconds,
    # with hedged requests it will last just around 1-2 second

    start = time.time()
    result = NODES['node'].query("SELECT hostName(), id FROM distributed SETTINGS receive_timeout={}".format(receive_timeout));
    query_time = time.time() - start

    assert TSV(result) == TSV(expected_replica + "\t1")

    print("Query time:", query_time)
    assert query_time < 10


def test_stuck_replica(started_cluster):
    cluster.pause_container("node_1")
    check_query(expected_replica="node_2")
    cluster.unpause_container("node_1")


def test_send_table_status_sleep(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=sleep_time, sleep_in_send_data=0))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_2")

def test_send_table_status_sleep2(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=sleep_time, sleep_in_send_data=0))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=sleep_time, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_3")


def test_send_data(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    time.sleep(2)
    
    check_query(expected_replica="node_2")


def test_send_data2(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_3")


def test_combination1(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=sleep_time, sleep_in_send_data=0))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    time.sleep(2)
    check_query(expected_replica="node_3")


def test_combination2(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=sleep_time, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_3")


def test_combination3(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=1, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=sleep_time))

    time.sleep(2)
    check_query(expected_replica="node_2")


def test_combination4(started_cluster):
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=1, sleep_in_send_data=sleep_time))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=1, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=2, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_2")


def test_receive_timeout1(started_cluster):
    # Check the situation when first two replicas get receive timeout
    # in establishing connection, but the third replica is ok.
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=3, sleep_in_send_data=0))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=3, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=1))

    time.sleep(2)
    check_query(expected_replica="node_3", receive_timeout=2)


def test_receive_timeout2(started_cluster):
    # Check the situation when first replica get receive timeout
    # in packet receiving but there are replicas in process of
    # connection establishing.
    NODES['node_1'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=0, sleep_in_send_data=4))

    NODES['node_2'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=2, sleep_in_send_data=0))

    NODES['node_3'].replace_config(
        '/etc/clickhouse-server/users.d/users1.xml',
        config.format(sleep_in_send_tables_status=2, sleep_in_send_data=0))

    time.sleep(2)
    check_query(expected_replica="node_2", receive_timeout=3)

