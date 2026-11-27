import re
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"node{num}", "shard": "shard"},
    )
    for num in range(3)
]


def _create_tables(table_name):
    nodes[0].query(
        f"DROP TABLE IF EXISTS {table_name} ON CLUSTER 'parallel_replicas'",
        settings={"database_atomic_wait_for_drop_and_detach_synchronously": True},
    )

    # big number of granules + low total size in bytes = super tiny granules = big min_marks_per_task
    # => big mark_segment_size will be chosen. it is not required to be big, just not equal to the default
    nodes[0].query(
        f"""
        CREATE TABLE {table_name} ON CLUSTER 'parallel_replicas' (value Int64)
        Engine=ReplicatedMergeTree('/test_parallel_replicas/shard/{table_name}', '{{replica}}')
        ORDER BY ()
        SETTINGS index_granularity = 1
        """
    )

    nodes[0].query(f"INSERT INTO {table_name} SELECT 42 FROM numbers(1000)")
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table_name}")


table_name = "t"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        _create_tables(table_name)
        yield cluster
    finally:
        cluster.shutdown()


# now mark_segment_size is part of the protocol and is communicated to the initiator.
# let's check that the correct value is actually used by the coordinator
@pytest.mark.parametrize("local_plan", [0, 1])
@pytest.mark.parametrize("index_analysis_only_on_coordinator", [0, 1])
def test_mark_segment_size_communicated_correctly(
    start_cluster, local_plan, index_analysis_only_on_coordinator
):

    query_id = f"query_id_{str(uuid.uuid4())}"
    nodes[0].query(
        f"SELECT sum(value) FROM {table_name}",
        settings={
            "allow_experimental_parallel_reading_from_replicas": 2,
            "max_parallel_replicas": 100,
            "cluster_for_parallel_replicas": "parallel_replicas",
            "parallel_replicas_mark_segment_size": 0,
            "parallel_replicas_local_plan": local_plan,
            "query_id": query_id,
            "parallel_replicas_index_analysis_only_on_coordinator": index_analysis_only_on_coordinator,
            "use_statistics_for_sum_avg_aggregation": 0,
        },
    )

    nodes[0].query("SYSTEM FLUSH LOGS")
    log_line = nodes[0].grep_in_log(f"{query_id}.*Reading state is fully initialized")
    assert re.search(r"mark_segment_size: (\d+)", log_line).group(1) == "16384"


def test_final_on_parallel_replicas(start_cluster):
    nodes[0].query("DROP TABLE IF EXISTS t0")
    nodes[0].query(
        "CREATE TABLE t0 (c0 Int) ENGINE = SummingMergeTree() ORDER BY tuple()",
    )
    nodes[0].query("INSERT INTO t0 VALUES (1)")
    nodes[0].query("OPTIMIZE TABLE t0 FINAL")
    resp = nodes[0].query(
        "SELECT 1 FROM t0 RIGHT JOIN (SELECT c0 FROM t0) tx ON TRUE GROUP BY c0",
        settings={
            "allow_experimental_parallel_reading_from_replicas": 2,
            "cluster_for_parallel_replicas": "parallel_replicas",
        },
    )
    assert resp == "1\n"
    nodes[0].query("DROP TABLE t0")


def test_insert_cluster_parallel_replicas(start_cluster):
    nodes[0].query("DROP TABLE IF EXISTS t0 ON CLUSTER 'test_cluster_two_shards'")
    nodes[0].query(
        "CREATE TABLE t0 ON CLUSTER 'test_cluster_two_shards' (c0 Int) ENGINE = Log()"
    )
    nodes[0].query(
        "INSERT INTO TABLE FUNCTION cluster('test_cluster_two_shards', 'default', 't0') (c0) SELECT c0 FROM generateRandom('c0 Int', 1) LIMIT 3",
        settings={
            "insert_distributed_one_random_shard": 1,
            "allow_experimental_parallel_reading_from_replicas": 2,
            "min_insert_block_size_bytes": 1,
            "optimize_trivial_insert_select": 1,
            "max_insert_threads": 3,
        },
    )
    nodes[0].query("DROP TABLE t0 ON CLUSTER 'test_cluster_two_shards'")


# https://github.com/ClickHouse/ClickHouse/issues/134035
# Only the initiator's query condition cache knows that the part without the projection has no
# matching rows. So the initiator reads the whole table from the projection in order, while the
# other replicas still read that part from the table in the Default mode.
def test_projection_read_in_order_with_warm_query_condition_cache(start_cluster):
    table = "t_projection_in_order_qcc"
    nodes[0].query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'parallel_replicas' SYNC")
    nodes[0].query(
        f"""
        CREATE TABLE {table} ON CLUSTER 'parallel_replicas'
            (k Int64, a Int64, b Int64, PROJECTION p (SELECT a, b ORDER BY a))
        ENGINE = ReplicatedMergeTree('/test_parallel_replicas/shard/{table}', '{{replica}}')
        ORDER BY k
        SETTINGS lightweight_mutation_projection_mode = 'rebuild'
        """
    )
    nodes[0].query(f"INSERT INTO {table} VALUES (-1, 5, 5)")
    nodes[0].query(f"DELETE FROM {table} WHERE k = -1", settings={"mutations_sync": 2})
    for node in nodes:
        node.query(f"SYSTEM STOP MERGES {table}")
    nodes[0].query(
        f"INSERT INTO {table} SELECT number, 1000000 - number, number % 7 FROM numbers(7000)",
        settings={"insert_quorum": 3},
    )
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table}")

    for node in nodes:
        # The deleted row's part has no projection, the other part has it.
        assert (
            node.query(
                f"SELECT rows, has_lightweight_delete, projections FROM system.parts "
                f"WHERE database = currentDatabase() AND table = '{table}' AND active ORDER BY name"
            )
            == "1\t1\t[]\n7000\t0\t['p']\n"
        )

    query = f"SELECT count(), sum(a) FROM (SELECT a FROM {table} ORDER BY a LIMIT 20)"
    for _ in range(3):
        for node in nodes:
            node.query("SYSTEM DROP QUERY CONDITION CACHE")

        expected = nodes[0].query(query)
        assert expected == "20\t19860210\n"
        assert int(nodes[0].query("SELECT count() FROM system.query_condition_cache")) >= 1
        for node in nodes[1:]:
            assert node.query("SELECT count() FROM system.query_condition_cache") == "0\n"

        assert (
            nodes[0].query(
                query,
                settings={
                    "enable_parallel_replicas": 1,
                    "max_parallel_replicas": 3,
                    "cluster_for_parallel_replicas": "parallel_replicas",
                },
            )
            == expected
        )

    nodes[0].query(f"DROP TABLE {table} ON CLUSTER 'parallel_replicas' SYNC")
