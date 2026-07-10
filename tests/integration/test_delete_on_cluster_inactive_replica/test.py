import time

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node1"},
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node2"},
    with_zookeeper=True,
    stay_alive=True,
)

DB = "test_delete_on_cluster_inactive_replica"
TABLE = "rmt"
CLUSTER = "test_cluster"
ZOOKEEPER_PATH = f"/clickhouse/tables/{DB}/{TABLE}"
REPLICA_IS_ACTIVE_PATH = f"{ZOOKEEPER_PATH}/replicas/node2/is_active"
SHARD_ID = "node1:9000,node2:9000"

DDL_SETTINGS = {
    "distributed_ddl_task_timeout": 5,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_keeper_path_absent(zk, path, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if zk.exists(path) is None:
            return
        time.sleep(0.5)

    raise AssertionError(f"Keeper path still exists: {path}")


def best_effort_restore_replica_and_drop_database():
    try:
        node2.restart_clickhouse()
    except Exception as ex:
        print(f"Failed to restart node2 during cleanup: {ex}")

    for node in (node1, node2):
        try:
            node.query(f"DROP DATABASE IF EXISTS {DB} SYNC", timeout=60)
        except Exception as ex:
            print(f"Failed to drop {DB} on {node.name} during cleanup: {ex}")


def get_last_lightweight_delete_task_entry():
    return node1.query(
        f"""
        SELECT entry
        FROM system.distributed_ddl_queue
        WHERE cluster = '{CLUSTER}'
          AND position(query, '{DB}') > 0
          AND position(query, '{TABLE}') > 0
          AND position(query, '_row_exists') > 0
        ORDER BY entry DESC
        LIMIT 1
        FORMAT TSVRaw
        """
    ).strip()


def test_delete_on_cluster_marks_task_executed_with_inactive_replica(started_cluster):
    try:
        node1.query(
            f"DROP DATABASE IF EXISTS {DB} ON CLUSTER {CLUSTER} SYNC",
            settings=DDL_SETTINGS,
        )
        node1.query(
            f"CREATE DATABASE {DB} ON CLUSTER {CLUSTER} ENGINE = Atomic",
            settings=DDL_SETTINGS,
        )
        node1.query(
            f"""
            CREATE TABLE {DB}.{TABLE} ON CLUSTER {CLUSTER}
            (
                id UInt64,
                value String
            )
            ENGINE = ReplicatedMergeTree('{ZOOKEEPER_PATH}', '{{replica}}')
            ORDER BY id
            """,
            settings=DDL_SETTINGS,
        )

        node1.query(f"INSERT INTO {DB}.{TABLE} VALUES (1, 'first'), (2, 'second')")
        node2.query(f"SYSTEM SYNC REPLICA {DB}.{TABLE}", timeout=30)

        zk = cluster.get_kazoo_client("zoo1")
        try:
            assert zk.exists(REPLICA_IS_ACTIVE_PATH) is not None
            zk.delete(REPLICA_IS_ACTIVE_PATH)
            wait_for_keeper_path_absent(zk, REPLICA_IS_ACTIVE_PATH)

            node1.query(
                f"DELETE FROM {DB}.{TABLE} ON CLUSTER {CLUSTER} WHERE id = 1",
                settings=DDL_SETTINGS,
                timeout=30,
            )

            entry = get_last_lightweight_delete_task_entry()
            assert entry
            assert (
                zk.exists(f"/clickhouse/task_queue/ddl/{entry}/shards/{SHARD_ID}/executed")
                is not None
            )
            assert sorted(zk.get_children(f"/clickhouse/task_queue/ddl/{entry}/finished")) == [
                "node1:9000",
                "node2:9000",
            ]
        finally:
            zk.stop()
            zk.close()
    finally:
        best_effort_restore_replica_and_drop_database()
