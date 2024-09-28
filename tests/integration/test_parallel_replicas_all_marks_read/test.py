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
    return (
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
    query_id = f"{table_name}_{randint(0, 1e9)}"

    assert _get_result_with_parallel_replicas(
        query, query_id, cluster_name, parallel_replicas_mark_segment_size
    ) == _get_result_without_parallel_replicas(query)

    assert _get_number_of_marks_read_by_replicas(
        query_id
    ) == _get_expected_amount_of_marks_to_read(query)
