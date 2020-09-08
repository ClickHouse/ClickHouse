import time

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


# This tests if the data directory for a table is cleaned up if there is a Zookeeper
# connection exception during a CreateQuery operation involving ReplicatedMergeTree tables.
# Test flow is as follows:
# 1. Configure cluster with ZooKeeper and create a database.
# 2. Drop all connections to ZooKeeper.
# 3. Try creating the table and there would be a Poco:Exception.
# 4. Try creating the table again and there should not be any error
# that indicates that the Directory for table already exists.


def test_replicated_zk_conn_failure():
    cluster = ClickHouseCluster(__file__)
    node1 = cluster.add_instance('node1', main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
    try:
        cluster.start()
        node1.query("CREATE DATABASE replica;")
        query_create = '''CREATE TABLE replica.test
        (
           id Int64,
           event_time DateTime
        )
        Engine=ReplicatedMergeTree('/clickhouse/tables/replica/test', 'node1')
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY id;'''.format(replica=node1.name)
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node1)
            time.sleep(5)
            error = node1.query_and_get_error(query_create)
            # Assert that there was net exception.
            assert "Poco::Exception. Code: 1000" in error
            # Assert that the exception was due to ZooKeeper connectivity.
            assert "All connection tries failed while connecting to ZooKeeper" in error
            # retry table creation
            error = node1.query_and_get_error(query_create)
            # Should not expect any errors related to directory already existing
            # and those should have been already cleaned up during the previous retry.
            assert "Directory for table data data/replica/test/ already exists" not in error
    finally:
        cluster.shutdown()
