import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=["configs/config.d/zookeeper_session_timeout.xml",
                                                    "configs/remote_servers.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE zktest ENGINE=Ordinary;")  # Different behaviour with Atomic
        node1.query(
            '''
            CREATE TABLE zktest.atomic_drop_table (n UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/zktest/tables/atomic_drop_table', 'node1')
            PARTITION BY n ORDER BY n
            '''
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_atomic_delete_with_stopped_zookeeper(start_cluster):
    node1.query("insert into zktest.atomic_drop_table values (8192)")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)
        error = node1.query_and_get_error("DROP TABLE zktest.atomic_drop_table")  # Table won't drop
        assert error != ""

    time.sleep(5)
    assert '8192' in node1.query("select * from zktest.atomic_drop_table")
