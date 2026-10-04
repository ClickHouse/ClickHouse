import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# node1 and node2 are replicas of shard 1, node3 and node4 are two replicas of shard 2.
# The `{shard}` macro puts the replicas of one shard on one `ReplicatedMergeTree`
# dataset, so an update that runs once per replica instead of once per shard shows up
# in the data.

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node1"},
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node2"},
    with_zookeeper=True,
    # Stopped and started again by `test_update_on_cluster_delayed_replica_does_not_reapply`,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "2", "replica": "node3"},
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "2", "replica": "node4"},
    with_zookeeper=True,
)

ALL_NODES = (node1, node2, node3, node4)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def unique_name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def replicated_engine(table):
    return f"ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')"


def create_table(cluster_name, table, engine):
    # Lightweight updates need both block columns, see `MergeTreeData::supportsLightweightUpdate`.
    node1.query(f"CREATE TABLE {table} ON CLUSTER {cluster_name} (id UInt64, v UInt64) ENGINE = {engine} ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")


def drop_table(cluster_name, table):
    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER {cluster_name} SYNC")


def sync_replicas(nodes, table):
    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA {table}")


def values(nodes, table):
    return {node.name: int(node.query(f"SELECT v FROM {table} WHERE id = 1")) for node in nodes}


def update_executions(nodes, query_id):
    # Count executions rather than patch parts. One execution can write several patch parts, and
    # merges can fold them. A DDL worker prefixes the query it runs with `/*ddl_entry=... */`
    # and gives it the `initial_query_id` of the statement that queued the entry. The client's
    # own `UPDATE ... ON CLUSTER` only queues the entry and has no prefix, so it is not counted.
    result = {}
    for node in nodes:
        node.query("SYSTEM FLUSH LOGS")
        result[node.name] = int(
            node.query(f"SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Update' AND initial_query_id = '{query_id}' AND startsWith(query, '/* ddl_entry=')")
        )
    return result


def test_update_on_cluster_runs_once_per_shard():
    table = unique_name("t_healthy")
    try:
        create_table("cluster_1x2", table, replicated_engine(table))
        # Insert once. Both replicas share this row. A second execution reads the first one's patch.
        node1.query(f"INSERT INTO {table} VALUES (1, 100)")
        sync_replicas((node2,), table)

        query_id = str(uuid.uuid4())
        node1.query(f"UPDATE {table} ON CLUSTER cluster_1x2 SET v = v + 1 WHERE id = 1", query_id=query_id)

        # The non-executing replica finishes the entry before it has fetched the patch.
        sync_replicas((node1, node2), table)

        executions = update_executions((node1, node2), query_id)
        assert {
            "values": values((node1, node2), table),
            "executions": sum(executions.values()),
        } == {"values": {"node1": 101, "node2": 101}, "executions": 1}, executions
    finally:
        drop_table("cluster_1x2", table)


def test_update_on_cluster_two_replicated_shards_run_once_per_shard():
    table = unique_name("t_two_shards")
    try:
        create_table("cluster_2x2", table, replicated_engine(table))
        # Each shard has its own copy of the row with a different value, so the result
        # shows that the update reached both shards and ran once on each.
        node1.query(f"INSERT INTO {table} VALUES (1, 100)")
        node3.query(f"INSERT INTO {table} VALUES (1, 200)")
        sync_replicas((node2, node4), table)

        query_id = str(uuid.uuid4())
        node1.query(f"UPDATE {table} ON CLUSTER cluster_2x2 SET v = v + 1 WHERE id = 1", query_id=query_id)
        sync_replicas(ALL_NODES, table)

        executions = update_executions(ALL_NODES, query_id)
        assert {
            "values": values(ALL_NODES, table),
            "shard 1 executions": executions["node1"] + executions["node2"],
            "shard 2 executions": executions["node3"] + executions["node4"],
        } == {
            "values": {"node1": 101, "node2": 101, "node3": 201, "node4": 201},
            "shard 1 executions": 1,
            "shard 2 executions": 1,
        }, executions
    finally:
        drop_table("cluster_2x2", table)


def test_plain_update_in_multi_shard_replicated_database_runs_once_per_shard():
    # A `Replicated` database with more than one shard replicates a plain `UPDATE` through its own
    # log (`DatabaseReplicated::shouldReplicateQuery`), so every replica of every shard receives it
    # and reaches the same routing decision as `ON CLUSTER`. The statement has no `ON CLUSTER` of
    # its own.
    database = unique_name("rdb")
    try:
        for node, shard, replica in zip(ALL_NODES, ("s1", "s1", "s2", "s2"), ("r1", "r2", "r1", "r2")):
            node.query(f"CREATE DATABASE {database} ENGINE = Replicated('/clickhouse/databases/{database}', '{shard}', '{replica}')")
        node1.query(f"CREATE TABLE {database}.t (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")

        for node in ALL_NODES:
            node.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
        node1.query(f"INSERT INTO {database}.t VALUES (1, 100)")
        node3.query(f"INSERT INTO {database}.t VALUES (1, 200)")
        sync_replicas(ALL_NODES, f"{database}.t")

        query_id = str(uuid.uuid4())
        node1.query(f"UPDATE {database}.t SET v = v + 1 WHERE id = 1", query_id=query_id)
        for node in ALL_NODES:
            node.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
        sync_replicas(ALL_NODES, f"{database}.t")

        executions = update_executions(ALL_NODES, query_id)
        assert {
            "values": values(ALL_NODES, f"{database}.t"),
            "shard s1 executions": executions["node1"] + executions["node2"],
            "shard s2 executions": executions["node3"] + executions["node4"],
        } == {
            "values": {"node1": 101, "node2": 101, "node3": 201, "node4": 201},
            "shard s1 executions": 1,
            "shard s2 executions": 1,
        }, executions
    finally:
        for node in ALL_NODES:
            node.query(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_update_on_cluster_non_replicated_runs_on_every_host():
    # Plain `MergeTree` tables hold independent data on each host, so every host still has to run
    # the update. This guards the `supportsReplication` condition of the single-replica routing.
    table = unique_name("t_non_replicated")
    try:
        create_table("cluster_1x2", table, "MergeTree")
        node1.query(f"INSERT INTO {table} VALUES (1, 100)")
        node2.query(f"INSERT INTO {table} VALUES (1, 500)")

        query_id = str(uuid.uuid4())
        node1.query(
            f"UPDATE {table} ON CLUSTER cluster_1x2 SET v = v + 1 WHERE id = 1",
            query_id=query_id,
        )

        assert {
            "values": values((node1, node2), table),
            "executions": update_executions((node1, node2), query_id),
        } == {
            "values": {"node1": 101, "node2": 501},
            "executions": {"node1": 1, "node2": 1},
        }
    finally:
        drop_table("cluster_1x2", table)


def test_update_on_cluster_delayed_replica_does_not_reapply():
    table = unique_name("t_delayed")
    try:
        create_table("cluster_1x2", table, replicated_engine(table))
        node1.query(f"INSERT INTO {table} VALUES (1, 100)")
        sync_replicas((node2,), table)

        query_id = str(uuid.uuid4())
        node2.stop_clickhouse()
        try:
            # `distributed_ddl_task_timeout = 0` returns as soon as the entry is queued instead of
            # waiting for the stopped host.
            node1.query(
                f"UPDATE {table} ON CLUSTER cluster_1x2 SET v = v + 1 WHERE id = 1",
                settings={"distributed_ddl_task_timeout": 0},
                query_id=query_id,
            )
            assert_eq_with_retry(
                node1,
                f"SELECT v FROM {table} WHERE id = 1",
                "101",
                retry_count=60,
                sleep_time=1,
            )
        finally:
            node2.start_clickhouse()

        # node2 picks up the entry it missed after it starts. It reports the entry as finished
        # whether it ran the update or saw that the other replica of the shard already had.
        assert_eq_with_retry(
            node1,
            f"SELECT status, exception_code FROM system.distributed_ddl_queue WHERE cluster = 'cluster_1x2' AND host = 'node2' AND startsWith(query, 'UPDATE') AND position(query, '{table}') > 0",
            "Finished\t0",
            retry_count=120,
            sleep_time=1,
        )
        sync_replicas((node1, node2), table)

        executions = update_executions((node1, node2), query_id)
        assert {
            "values": values((node1, node2), table),
            "executions": sum(executions.values()),
        } == {"values": {"node1": 101, "node2": 101}, "executions": 1}, executions
    finally:
        drop_table("cluster_1x2", table)
