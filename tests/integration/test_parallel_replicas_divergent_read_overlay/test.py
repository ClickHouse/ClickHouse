# A replica reads a part with its own patch parts (lightweight updates) and on-the-fly mutations applied.
# When replicas apply different overlays to the same part (a patch part not yet fetched by one replica,
# a mutation pending on one node only), a parallel-replicas query must return the snapshot replica's data,
# not a mix of updated and not updated rows.

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
rows = 262144

nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"node{num}", "shard": "shard"},
    )
    for num in range(3)
]

table_settings = (
    "index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, "
    "enable_block_number_column = 1, enable_block_offset_column = 1"
)

# 4096 marks in segments of 128 marks: every replica owns segments of the single part.
parallel_settings = {
    "enable_parallel_replicas": 2,
    "max_parallel_replicas": 3,
    "cluster_for_parallel_replicas": cluster_name,
    "parallel_replicas_mark_segment_size": 128,
    "merge_tree_min_read_task_size": 1,
    "merge_tree_min_rows_for_concurrent_read": 0,
    "merge_tree_min_bytes_for_concurrent_read": 0,
    "max_threads": 1,
}

query = "SELECT count(), sum(b) FROM {table} WHERE NOT ignore(sleepEachRow(0.00002))"
in_order_query = (
    "SELECT count(), sum(b) FROM "
    "(SELECT a, b FROM {table} WHERE NOT ignore(sleepEachRow(0.00002)) ORDER BY a LIMIT 1000000)"
)

updated = f"{rows}\t{rows}\n"
not_updated = f"{rows}\t0\n"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def local_sum(node, table, settings=None):
    return node.query(
        f"SELECT count(), sum(b) FROM {table}",
        settings={"enable_parallel_replicas": 0, **(settings or {})},
    )


def follower_read_rows(node, query_id):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            f"SELECT max(read_rows) FROM system.query_log "
            f"WHERE initial_query_id = '{query_id}' AND is_initial_query = 0"
        )
    )


def test_replicated_patch_part_lag(start_cluster):
    table = "t_patch_lag"
    zk_path = f"/clickhouse/tables/{table}_{uuid.uuid4().hex}"
    try:
        for node in nodes:
            node.query(
                f"CREATE TABLE {table} (a UInt64, b UInt64) "
                f"ENGINE = ReplicatedMergeTree('{zk_path}', '{{replica}}') ORDER BY a SETTINGS {table_settings}"
            )
            node.query(f"SYSTEM STOP MERGES {table}")

        nodes[0].query(f"INSERT INTO {table} SELECT number, 0 FROM numbers({rows})")
        for node in nodes:
            node.query(f"SYSTEM SYNC REPLICA {table}")

        # node1 does not fetch the patch part of the update.
        nodes[1].query(f"SYSTEM STOP FETCHES {table}")
        nodes[0].query(f"UPDATE {table} SET b = 1 WHERE 1")
        nodes[2].query(f"SYSTEM SYNC REPLICA {table}")

        assert [local_sum(node, table) for node in nodes] == [
            updated,
            not_updated,
            updated,
        ]
        assert (
            nodes[1].query(
                f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND startsWith(name, 'patch')"
            )
            == "0\n"
        )

        assert (
            nodes[0].query(query.format(table=table), settings=parallel_settings)
            == updated
        )
        assert (
            nodes[1].query(query.format(table=table), settings=parallel_settings)
            == not_updated
        )
        assert (
            nodes[2].query(query.format(table=table), settings=parallel_settings)
            == updated
        )
        assert nodes[0].query(
            query.format(table=table),
            settings={**parallel_settings, "parallel_replicas_local_plan": 0},
        ) in (updated, not_updated)

        # The in-sync replica keeps reading, the lagging one does not, with both coordinators.
        query_id = f"patch_lag_{uuid.uuid4().hex}"
        assert (
            nodes[0].query(
                query.format(table=table), settings=parallel_settings, query_id=query_id
            )
            == updated
        )
        assert follower_read_rows(nodes[2], query_id) > 0
        assert follower_read_rows(nodes[1], query_id) == 0
        query_id = f"patch_lag_in_order_{uuid.uuid4().hex}"
        assert (
            nodes[0].query(
                in_order_query.format(table=table),
                settings={**parallel_settings, "optimize_read_in_order": 1},
                query_id=query_id,
            )
            == updated
        )
        assert follower_read_rows(nodes[2], query_id) > 0
        assert follower_read_rows(nodes[1], query_id) == 0
    finally:
        nodes[1].query(f"SYSTEM START FETCHES {table}")
        for node in nodes:
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def create_plain_tables(table):
    for node in nodes:
        node.query(
            f"CREATE TABLE {table} (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a SETTINGS {table_settings}"
        )
        node.query(f"SYSTEM STOP MERGES {table}")
        node.query(f"INSERT INTO {table} SELECT number, 0, 0 FROM numbers({rows})")


# Each node of plain MergeTree tables applies only its own overlay: none on node0, `b = 1` on node1, `c = 1` on node2.
plain_query = "SELECT count(), sum(b), sum(c) FROM {table} WHERE NOT ignore(sleepEachRow(0.00002))"
plain_results = [f"{rows}\t0\t0\n", f"{rows}\t{rows}\t0\n", f"{rows}\t0\t{rows}\n"]


def check_plain_tables(table, settings):
    for node, result in zip(nodes, plain_results):
        assert (
            node.query(
                f"SELECT count(), sum(b), sum(c) FROM {table}",
                settings={"enable_parallel_replicas": 0, **settings},
            )
            == result
        )

    settings = {
        **parallel_settings,
        **settings,
        "parallel_replicas_for_non_replicated_merge_tree": 1,
    }
    for node, result in zip(nodes, plain_results):
        assert node.query(plain_query.format(table=table), settings=settings) == result


def test_on_fly_mutations_differ(start_cluster):
    table = "t_on_fly"
    try:
        create_plain_tables(table)
        # The mutations stay pending under STOP MERGES.
        nodes[1].query(
            f"ALTER TABLE {table} UPDATE b = 1 WHERE 1 SETTINGS mutations_sync = 0"
        )
        nodes[2].query(
            f"ALTER TABLE {table} UPDATE c = 1 WHERE 1 SETTINGS mutations_sync = 0"
        )

        check_plain_tables(table, {"apply_mutations_on_fly": 1})

        # node0 gets the same pending mutation as node1: node1 keeps reading, node2 does not.
        nodes[0].query(
            f"ALTER TABLE {table} UPDATE b = 1 WHERE 1 SETTINGS mutations_sync = 0"
        )
        query_id = f"on_fly_{uuid.uuid4().hex}"
        assert (
            nodes[0].query(
                plain_query.format(table=table),
                settings={
                    **parallel_settings,
                    "parallel_replicas_for_non_replicated_merge_tree": 1,
                    "apply_mutations_on_fly": 1,
                },
                query_id=query_id,
            )
            == plain_results[1]
        )
        assert follower_read_rows(nodes[1], query_id) > 0
        assert follower_read_rows(nodes[2], query_id) == 0
    finally:
        for node in nodes:
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_patch_parts_differ(start_cluster):
    table = "t_patches"
    try:
        create_plain_tables(table)
        nodes[1].query(f"UPDATE {table} SET b = 1 WHERE 1")
        nodes[2].query(f"UPDATE {table} SET c = 1 WHERE 1")

        patches = [
            node.query(
                f"SELECT name FROM system.parts WHERE table = '{table}' AND active AND startsWith(name, 'patch')"
            ).split()
            for node in nodes
        ]
        assert [len(p) for p in patches] == [0, 1, 1] and patches[1] != patches[2]

        check_plain_tables(table, {})
    finally:
        for node in nodes:
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")
