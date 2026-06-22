import time

import pytest

from helpers.cluster import ClickHouseCluster


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                DROP DATABASE IF EXISTS test SYNC;
                CREATE DATABASE test;
    
                CREATE TABLE test.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )

        node.query(
            """
                DROP DATABASE IF EXISTS test1 SYNC;
                CREATE DATABASE test1;
    
                CREATE TABLE test1.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test1/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )

        node.query(
            """
                DROP DATABASE IF EXISTS test2 SYNC;
                CREATE DATABASE test2;
    
                CREATE TABLE test2.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test2/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )

        node.query(
            """
            DROP DATABASE IF EXISTS test3 SYNC;
            CREATE DATABASE test3;
    
                CREATE TABLE test3.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test3/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )

        node.query(
            """
                DROP DATABASE IF EXISTS test4 SYNC;
                CREATE DATABASE test4;
    
                CREATE TABLE test4.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test4/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node_1_1 = cluster.add_instance(
    "node_1_1", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
node_1_2 = cluster.add_instance(
    "node_1_2", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
node_1_3 = cluster.add_instance(
    "node_1_3", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def check_exists(zk, path):
    zk.sync(path)
    return zk.exists(path)


def test_drop_replica(start_cluster):
    fill_nodes([node_1_1, node_1_2], 1)

    node_1_1.query(
        "INSERT INTO test.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node_1_1.query(
        "INSERT INTO test1.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node_1_1.query(
        "INSERT INTO test2.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node_1_1.query(
        "INSERT INTO test3.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node_1_1.query(
        "INSERT INTO test4.test_table SELECT number, toString(number) FROM numbers(100)"
    )

    zk = cluster.get_kazoo_client("zoo1")
    assert "can't drop local replica" in node_1_1.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1'"
    )
    assert "can't drop local replica" in node_1_1.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test"
    )
    assert "can't drop local replica" in node_1_1.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table"
    )
    assert "it's active" in node_1_2.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1'"
    )
    assert "it's active" in node_1_2.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test"
    )
    assert "it's active" in node_1_2.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table"
    )
    assert "it's active" in node_1_3.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test/{shard}/replicated/test_table'".format(
            shard=1
        )
    )

    assert "There is a local table" in node_1_1.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test/{shard}/replicated/test_table'".format(
            shard=1
        )
    )
    assert "does not look like a table path" in node_1_3.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test'"
    )

    node_1_1.query("DETACH DATABASE test")
    for i in range(1, 5):
        node_1_1.query("DETACH DATABASE test{}".format(i))

    assert "does not exist" in node_1_3.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table"
    )

    assert "does not exist" in node_1_3.query_and_get_error(
        "SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test1"
    )

    node_1_3.query("SYSTEM DROP REPLICA 'node_1_1'")
    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/test3/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_1"
        ),
    )
    assert exists_replica_1_1 != None

    ## If you want to drop a inactive/stale replicate table that does not have a local replica, you can following syntax(ZKPATH):
    node_1_3.query(
        "SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test2/{shard}/replicated/test_table'".format(
            shard=1
        )
    )
    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/test2/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_1"
        ),
    )
    assert exists_replica_1_1 == None

    node_1_2.query("SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table")
    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/test/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_1"
        ),
    )
    assert exists_replica_1_1 == None

    node_1_2.query("SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test1")
    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/test1/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_1"
        ),
    )
    assert exists_replica_1_1 == None

    node_1_3.query(
        "SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test3/{shard}/replicated/test_table'".format(
            shard=1
        )
    )
    exists_replica_1_1 = check_exists(
        zk,
        "/clickhouse/tables/test3/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_1"
        ),
    )
    assert exists_replica_1_1 == None

    node_1_1.query("ATTACH DATABASE test4")

    node_1_2.query("DETACH TABLE test4.test_table")
    node_1_1.query(
        "SYSTEM DROP REPLICA 'node_1_2' FROM ZKPATH '/clickhouse/tables/test4/{shard}/replicated/test_table'".format(
            shard=1
        )
    )
    exists_replica_1_2 = check_exists(
        zk,
        "/clickhouse/tables/test4/{shard}/replicated/test_table/replicas/{replica}".format(
            shard=1, replica="node_1_2"
        ),
    )
    assert exists_replica_1_2 == None

    node_1_1.query("ATTACH DATABASE test")
    for i in range(1, 4):
        node_1_1.query("ATTACH DATABASE test{}".format(i))
