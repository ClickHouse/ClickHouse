# `allow_feature_tier = 1` makes a replica drop the `automatic_parallel_replicas_mode = 0` that the initiator ships,
# so every replica keeps `automatic_parallel_replicas_mode = 1` from its profile.
# Changes of `EXPERIMENTAL` settings are rejected in queries, so they are set in `configs/users.xml` only.

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=["configs/remote_servers.xml", "configs/feature_tier.xml"],
        user_configs=["configs/users.xml"],
        with_zookeeper=True,
    )
    for i in (1, 2, 3)
]
node1 = nodes[0]

CLUSTER = "test_1_shard_3_replicas"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(name, rows, table_settings=""):
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE TABLE {name} (key UInt64, value String) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}', 'r{i}') ORDER BY key {table_settings}"
        )
    if rows:
        node1.query(
            f"INSERT INTO {name} SELECT number, toString(cityHash64(number)) FROM numbers({rows})"
        )
        for node in nodes[1:]:
            node.query(f"SYSTEM SYNC REPLICA {name}")


def test_insert_select(start_cluster):
    rows = 100000
    create_table("t_source", rows)
    create_table("t_target", 0)

    query_id = str(uuid.uuid4())
    node1.query(
        "INSERT INTO t_target SELECT * FROM t_source",
        settings={
            "enable_parallel_replicas": 1,
            "parallel_distributed_insert_select": 2,
            "cluster_for_parallel_replicas": CLUSTER,
            "max_parallel_replicas": 3,
            "deduplicate_insert_select": "disable",
        },
        query_id=query_id,
    )
    node1.query("SYSTEM SYNC REPLICA t_target LIGHTWEIGHT")
    assert (
        node1.query("SELECT count(), uniqExact(key) FROM t_target")
        == f"{rows}\t{rows}\n"
    )

    for node in nodes:
        node.query("SYSTEM FLUSH LOGS query_log")
    followers = node1.query(
        f"SELECT count() FROM clusterAllReplicas({CLUSTER}, system.query_log) "
        f"WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish' AND query_kind = 'Insert' AND NOT is_initial_query"
    )
    # Without it a local fallback of the INSERT would pass the row count above.
    assert int(followers) == 2


def test_select(start_cluster):
    rows = 125000
    groups = 1000
    create_table("t", rows, "SETTINGS index_granularity = 128")

    query = f"SELECT key % {groups} AS g, count() FROM t GROUP BY g HAVING count() != {rows // groups}"
    settings = {
        "enable_parallel_replicas": 1,
        "cluster_for_parallel_replicas": CLUSTER,
        "max_parallel_replicas": 3,
        "parallel_replicas_local_plan": 1,
        "max_threads": 4,
        "max_block_size": 128,
        "merge_tree_min_bytes_per_task_for_remote_reading": 0,
        "max_bytes_before_external_group_by": 0,
        "max_bytes_ratio_before_external_group_by": 0,
    }

    # The first run collects the statistics, the second one is switched to parallel replicas.
    assert node1.query(query, settings=settings) == ""

    # Replicas that did not join the coordinated read are not cancelled, so their results would be merged.
    node1.query("SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas")
    query_id = str(uuid.uuid4())
    assert node1.query(query, settings=settings, query_id=query_id) == ""

    node1.query("SYSTEM FLUSH LOGS query_log")
    used_replicas = node1.query(
        f"SELECT ProfileEvents['ParallelReplicasUsedCount'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert int(used_replicas) >= 1
