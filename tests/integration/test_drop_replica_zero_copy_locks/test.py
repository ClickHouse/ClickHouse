import pytest

from helpers.cluster import ClickHouseCluster


def fill_nodes_zero_copy(nodes, shard):
    for node in nodes:
        node.query(
            """
                DROP DATABASE IF EXISTS test_zero_copy SYNC;
                CREATE DATABASE test_zero_copy;

                CREATE TABLE test_zero_copy.test_table_1 UUID '10000000-0000-0000-0000-000000000001' (date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table_1', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date)
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0,
                    cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )
        node.query(
            """
                CREATE TABLE test_zero_copy.test_table_2 UUID '10000000-0000-0000-0000-000000000002' (date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table_2', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date)
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0,
                    cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )
    node.query("INSERT INTO test_zero_copy.test_table_1 VALUES (toDate('2025-06-01'), 1), (toDate('2025-07-02'), 2)")
    node.query("INSERT INTO test_zero_copy.test_table_2 VALUES (toDate('2025-06-01'), 1), (toDate('2025-07-02'), 2)")


cluster = ClickHouseCluster(__file__)
configs = ["configs/test_config.xml"]
node_1_1 = cluster.add_instance(
    "node_1_1", with_zookeeper=True, main_configs=configs, with_minio=True
)
node_1_2 = cluster.add_instance(
    "node_1_2", with_zookeeper=True, main_configs=configs, with_minio=True
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


def check_children(zk, path, children_to_check):
    zk.sync(path)
    children = zk.get_children(path)
    assert set(children_to_check) == set(children)


def test_drop_replica_zero_copy_locks(start_cluster):
    fill_nodes_zero_copy([node_1_1, node_1_2], "shard1")

    zk = cluster.get_kazoo_client("zoo1")
    zk_path = "/clickhouse/zero_copy/zero_copy_s3"
    table_path = f"{zk_path}/10000000-0000-0000-0000-000000000001"
    part_path = f"{table_path}/202506_0_0_0"
    check_children(
        zk,
        table_path,
        ["202506_0_0_0", "202507_0_0_0"]
    )

    blob_path = zk.get_children(part_path)[0]
    check_children(
        zk,
        f"{part_path}/{blob_path}",
        ["node_1_1", "node_1_2"]
    )

    node_1_2.query("DETACH TABLE test_zero_copy.test_table_1")
    node_1_2.query("SYSTEM DROP REPLICA 'node_1_2' FROM ZKPATH '/clickhouse/tables/test/shard1/replicated/test_table_1'")

    check_children(
        zk,
        f"{part_path}/{blob_path}",
        ["node_1_1"]
    )

    node_1_1.query("DETACH TABLE test_zero_copy.test_table_1")
    node_1_1.query("SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test/shard1/replicated/test_table_1'")

    check_children(
        zk,
        zk_path,
        ["10000000-0000-0000-0000-000000000002"]
    )
