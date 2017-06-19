import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

instance_with_dist_table = cluster.add_instance('instance_with_dist_table', main_configs=['configs/remote_servers.xml'])
remote = cluster.add_instance('remote')

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        remote.query("CREATE TABLE local (x UInt32) ENGINE = Log")

        instance_with_dist_table.query('''
CREATE TABLE distributed (x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local')
''')

        yield cluster

    finally:
        cluster.shutdown()


def test_reconnect(started_cluster):
    with PartitionManager() as pm:
        # Open a connection for insertion.
        instance_with_dist_table.query("INSERT INTO distributed VALUES (1)")
        time.sleep(0.5)
        assert remote.query("SELECT count(*) FROM local").strip() == '1'

        # Now break the connection.
        pm.partition_instances(instance_with_dist_table, remote, action='REJECT --reject-with tcp-reset')
        instance_with_dist_table.query("INSERT INTO distributed VALUES (2)")
        time.sleep(0.5)

        # Heal the partition and insert more data.
        # The connection must be reestablished and after some time all data must be inserted.
        pm.heal_all()
        instance_with_dist_table.query("INSERT INTO distributed VALUES (3)")
        time.sleep(0.5)
        assert remote.query("SELECT count(*) FROM local").strip() == '3'
