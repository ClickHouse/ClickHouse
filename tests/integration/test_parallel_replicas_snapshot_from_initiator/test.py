import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{num}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for num in range(6)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(table_name):
    # Restart replicated sends that may have been stopped by a previous run
    for node in nodes:
        node.query("SYSTEM START REPLICATED SENDS")

    # Drop all replicas first so the ZK path is fully cleaned up
    # before recreating. If we interleave DROP and CREATE in the same loop,
    # a newly created replica will see stale replicas still in ZK and
    # fetch their old data, causing accumulation across repeated runs.
    for node in nodes:
        node.query(
            f"DROP TABLE IF EXISTS {table_name}",
            settings={"database_atomic_wait_for_drop_and_detach_synchronously": True},
        )

    for idx, node in enumerate(nodes):
        node.query(
            f"""
            CREATE TABLE {table_name} (value Int64)
            Engine=ReplicatedMergeTree('/test_parallel_replicas/shard/{table_name}', '{idx}')
            ORDER BY ()
            """
        )

    nodes[0].query(
        f"INSERT INTO {table_name} SELECT * FROM numbers(1000)",
        settings={"insert_deduplicate": 0},
    )
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table_name}")

    for idx, node in enumerate(nodes):
        node.query("SYSTEM STOP REPLICATED SENDS")
        # the same data on all nodes except for a single value
        node.query(
            f"INSERT INTO {table_name} VALUES ({idx})",
            settings={"insert_deduplicate": 0},
        )


# check that we use the state of data parts from the initiator node (for some sort of determinism of what is been read).
# currently it is implemented only when we build local plan for the initiator node (we aim to make this behavior default)
def test_initiator_snapshot_is_used_for_reading(start_cluster):
    table_name = "t"
    _create_tables(table_name)

    for idx, node in enumerate(nodes):
        expected = 499500 + idx  # sum of all integers 0..999 + idx
        assert (
            node.query(
                f"SELECT sum(value) FROM {table_name}",
                settings={
                    "allow_experimental_parallel_reading_from_replicas": 2,
                    "max_parallel_replicas": 100,
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "parallel_replicas_local_plan": True,
                    "enable_shared_storage_snapshot_in_query": False,
                },
            )
            == f"{expected}\n"
        )


# While an `ALTER` that changes a column is propagating, replicas of the same table have different metadata
# (a lagging `ReplicatedMergeTree` replica, or any `SharedMergeTree` replica that has not pulled the new metadata yet).
# A query with parallel replicas that reads the changed column currently fails in both implementations.
# The expected errors document the current behavior, the goal is to make these queries succeed.
ALTER_CASES = [
    # (alter, query, query-based result or error, plan-based result or error)
    ("ADD COLUMN c Int64 DEFAULT value * 2", "SELECT sum(c) FROM {t}", "9999900000", "9999900000"),
    (
        "MODIFY COLUMN extra Int32",
        "SELECT sum(extra) FROM {t}",
        "Conversion from AggregateFunction(sum, Int64) to AggregateFunction(sum, Int32) is not supported",
        "INCOMPATIBLE_COLUMNS",
    ),
    ("RENAME COLUMN extra TO e2", "SELECT sum(e2) FROM {t}", "UNKNOWN_IDENTIFIER", "NOT_FOUND_COLUMN_IN_BLOCK"),
]


@pytest.mark.parametrize("alter,query,query_based,plan_based", ALTER_CASES)
def test_select_while_alter_is_not_applied_on_replicas(start_cluster, alter, query, query_based, plan_based):
    t = "t_alter_" + alter.split()[0].lower()
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {t} SYNC")
    for idx, node in enumerate(nodes):
        node.query(
            f"""
            CREATE TABLE {t} (value Int64, extra Int64 DEFAULT 1)
            Engine=ReplicatedMergeTree('/test_parallel_replicas/alter/{t}', '{idx}')
            ORDER BY value
            """
        )
    nodes[0].query(f"INSERT INTO {t} (value) SELECT number FROM numbers(100000)")
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {t}")

    # The initiator applies the `ALTER`, the other replicas do not
    for node in nodes[1:]:
        node.query(f"SYSTEM STOP REPLICATION QUEUES {t}")
    try:
        nodes[0].query(f"ALTER TABLE {t} {alter}", settings={"alter_sync": 0})
        nodes[0].query(f"SYSTEM SYNC REPLICA {t}")

        columns_query = f"SELECT groupArray(name || ' ' || type) FROM system.columns WHERE database = currentDatabase() AND table = '{t}'"
        assert nodes[1].query(columns_query) != nodes[0].query(columns_query)

        for plan_based_setting, expected in ((0, query_based), (1, plan_based)):
            result, error = nodes[0].query_and_get_answer_with_error(
                query.format(t=t),
                settings={
                    "enable_analyzer": 1,
                    "enable_parallel_replicas": 1,
                    "max_parallel_replicas": 100,
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "parallel_replicas_plan_based": plan_based_setting,
                },
            )
            if expected.isdigit():
                assert error == "" and result == expected + "\n", (plan_based_setting, error)
            else:
                assert expected in error, (plan_based_setting, result, error)
    finally:
        for node in nodes[1:]:
            node.query(f"SYSTEM START REPLICATION QUEUES {t}")
