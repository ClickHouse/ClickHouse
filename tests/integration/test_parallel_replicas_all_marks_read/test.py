import json
from random import randint

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas_with_unavailable_nodes"

nodes = [
    cluster.add_instance(
        f"node{num}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for num in range(3)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(table_name, table_size, index_granularity):
    for num in range(len(nodes)):
        nodes[num].query(f"DROP TABLE IF EXISTS {table_name}")

        nodes[num].query(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String)
            Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', '{num}')
            ORDER BY (key)
            SETTINGS index_granularity = {index_granularity}
            """
        )

    nodes[0].query(
        f"""
        INSERT INTO {table_name}
        SELECT number, toString(number) FROM numbers_mt({table_size})
        """
    )


def _create_query(query_tmpl, table_name):
    rand_set = [randint(0, 500) for i in range(42)]
    return query_tmpl.format(table_name=table_name, rand_set=rand_set)


def _get_result_without_parallel_replicas(query):
    return nodes[0].query(
        query,
        settings={
            "enable_parallel_replicas": 0,
        },
    )


def _get_result_with_parallel_replicas(
    query, query_id, cluster_name, parallel_replicas_mark_segment_size
):
    return nodes[0].query(
        query,
        settings={
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": 6,
            "cluster_for_parallel_replicas": f"{cluster_name}",
            "parallel_replicas_mark_segment_size": parallel_replicas_mark_segment_size,
            "query_id": query_id,
            "parallel_replicas_index_analysis_only_on_coordinator": False,
        },
    )


def _get_expected_amount_of_marks_to_read(query):
    return json.loads(
        nodes[0].query(
            f"""
            EXPLAIN ESTIMATE
            {query}
            FORMAT JSONEachRow
            """
        )
    )["marks"]


def _get_number_of_marks_read_by_replicas(query_id):
    nodes[0].query("SYSTEM FLUSH LOGS")
    return int(
        nodes[0]
        .query(
            f"""
                SELECT sum(
                    ProfileEvents['ParallelReplicasReadAssignedMarks']
                    + ProfileEvents['ParallelReplicasReadUnassignedMarks']
                    + ProfileEvents['ParallelReplicasReadAssignedForStealingMarks']
                )
                FROM system.query_log
                WHERE query_id = '{query_id}'
                """
        )
        .strip()
    )


@pytest.mark.parametrize(
    "query_tmpl",
    [
        "SELECT sum(cityHash64(*)) FROM {table_name}",
        "SELECT sum(cityHash64(*)) FROM {table_name} WHERE intDiv(key, 100) IN {rand_set}",
    ],
)
@pytest.mark.parametrize(
    "table_size",
    [1000, 10000, 100000],
)
@pytest.mark.parametrize(
    "index_granularity",
    [10, 100],
)
@pytest.mark.parametrize(
    "parallel_replicas_mark_segment_size",
    [1, 10],
)
def test_number_of_marks_read(
    start_cluster,
    query_tmpl,
    table_size,
    index_granularity,
    parallel_replicas_mark_segment_size,
):
    if nodes[0].is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers (too slow)")

    table_name = f"tbl_{len(query_tmpl)}_{cluster_name}_{table_size}_{index_granularity}_{parallel_replicas_mark_segment_size}"
    _create_tables(table_name, table_size, index_granularity)

    if "where" in query_tmpl.lower():
        # We need all the replicas to see the same state of parts to make sure that index analysis will pick the same amount of marks for reading
        # regardless of which replica's state will be chosen as the working set. This should became redundant once we start to always use initiator's snapshot.
        nodes[0].query(f"OPTIMIZE TABLE {table_name} FINAL", settings={"alter_sync": 2})
        for node in nodes:
            node.query(f"SYSTEM SYNC REPLICA {table_name} STRICT")

    query = _create_query(query_tmpl, table_name)
    query_id = f"{table_name}_{randint(0, 10**9)}"

    assert _get_result_with_parallel_replicas(
        query, query_id, cluster_name, parallel_replicas_mark_segment_size
    ) == _get_result_without_parallel_replicas(query)

    assert _get_number_of_marks_read_by_replicas(
        query_id
    ) == _get_expected_amount_of_marks_to_read(query)


def test_query_condition_cache_of_each_replica(start_cluster):
    # Each replica has its own query condition cache, but the coordinator assigns ranges from its own analysis,
    # where only its own cache is consulted. A follower has to apply its cache to the ranges it is assigned,
    # or it reads its share of the table again on every run.
    table_name = f"tbl_query_condition_cache_{randint(0, 10**9)}"
    _create_tables(table_name, 0, 8192)
    nodes[0].query(
        f"""
        INSERT INTO {table_name}
        SELECT number, if(number % 1000000 = 7, 'needle', toString(number)) FROM numbers_mt(10000000)
        """
    )
    nodes[0].query(f"OPTIMIZE TABLE {table_name} FINAL", settings={"alter_sync": 2})
    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA {table_name} STRICT")
        node.query("SYSTEM DROP QUERY CONDITION CACHE")

    query = f"SELECT count() FROM {table_name} WHERE value LIKE '%needle%'"
    settings = {
        "enable_parallel_replicas": 1,
        # Otherwise the second run may be switched to a single node.
        "automatic_parallel_replicas_mode": 0,
        "max_parallel_replicas": 2,
        "cluster_for_parallel_replicas": "parallel_replicas_two_nodes",
        "parallel_replicas_min_number_of_rows_per_replica": 0,
        "use_query_condition_cache": 1,
        "max_threads": 4,
    }

    query_ids = [f"{table_name}_{run}" for run in range(2)]
    for query_id in query_ids:
        assert nodes[0].query(query, settings={**settings, "query_id": query_id}) == "10\n"

    for node in nodes[:2]:
        node.query("SYSTEM FLUSH LOGS query_log")

    def rows_read_by_follower(query_id):
        return int(
            nodes[1].query(
                f"""
                SELECT sum(read_rows) FROM system.query_log
                WHERE initial_query_id = '{query_id}' AND NOT is_initial_query AND type = 'QueryFinish'
                """
            )
        )

    # The first run reads everything, so after it each granule is in the cache of the replica that read it.
    assert rows_read_by_follower(query_ids[0]) > 0
    # The second run reads only the granules with matches: at most the 10 granules of 8192 rows.
    assert rows_read_by_follower(query_ids[1]) <= 10 * 8192
