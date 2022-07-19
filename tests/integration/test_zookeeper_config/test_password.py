

import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="password")

# TODO ACL not implemented in Keeper.
node1 = cluster.add_instance('node1', with_zookeeper=True,
                             main_configs=["configs/remote_servers.xml", "configs/zookeeper_config_with_password.xml"])
                                
node2 = cluster.add_instance('node2', with_zookeeper=True, main_configs=["configs/remote_servers.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_identity(started_cluster):
    node1.query('DROP TABLE IF EXISTS simple SYNC')

    node1.query('''
    CREATE TABLE simple (date Date, id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}', date, id, 8192);
    '''.format(replica=node1.name))

    with pytest.raises(Exception):
        node2.query('''
        CREATE TABLE simple (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '1', date, id, 8192);
        ''')
