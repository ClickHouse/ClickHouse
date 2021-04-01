import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query('''
            CREATE TABLE default.t1_local
            (
                event_date Date DEFAULT toDate(event_time),
                event_time DateTime,
                log_type UInt32,
                account_id String
            )
            ENGINE = MergeTree(event_date, (event_time, account_id), 8192);
            ''')

            node.query('''
            CREATE TABLE default.t1 AS default.t1_local
            ENGINE = Distributed('two_shards', 'default', 't1_local', rand());
            ''')

        yield cluster

    finally:
        cluster.shutdown()


def test_read(started_cluster):
    assert node1.query('''SELECT event_date, event_time, log_type
                        FROM default.t1
                        WHERE (log_type = 30305) AND (account_id = '111111')
                        LIMIT 1''').strip() == ''
