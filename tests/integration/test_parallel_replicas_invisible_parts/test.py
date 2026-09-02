# We had a couple of bugs where difference in snapshots on replicas (totally normal situation) lead to wrong query result:
# #58722 - some marks were assigned more than once to different replicas because mark segments were not properly cleaned up after moving between queues
# #58844 - replica was assigned reading from a part it didn't see
# In this test we emulate a situation when each replica has unique snapshot of data parts.
# Specifically each of 5 nodes sees 10 parts, half of these parts is common for all nodes, the other half is unique for the specific node.
# This way we will trigger the logic that caused problems in the first issue, because none of the nodes will have all the parts from main node's snapshot in its own snapshot,
# also there is a good chance to schedule reading from one of unique parts to a replica that doesn't see them if something is off with visibility checks.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
total_parts = 10

nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"node{num}", "shard": "shard"},
    )
    for num in range(5)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables(table_name, table_size, index_granularity):

    nodes[0].query(
        f"""
        CREATE TABLE {table_name} ON CLUSTER '{cluster_name}' (key Int64, value String)
        Engine=ReplicatedMergeTree('/test_parallel_replicas/shard/{table_name}/', '{{replica}}')
        ORDER BY (key)
        SETTINGS index_granularity = {index_granularity}, max_bytes_to_merge_at_max_space_in_pool = 0, max_bytes_to_merge_at_max_space_in_pool = 1
        """
    )

    assert table_size % total_parts == 0 and total_parts % 2 == 0
    for i in range(total_parts // 2):
        nodes[0].query(
            f"""
            INSERT INTO {table_name}
            SELECT number, toString(number) FROM numbers_mt({table_size / total_parts})
            SETTINGS insert_deduplicate = 0
            """
        )

    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table_name}")

    nodes[0].query(f"SYSTEM STOP FETCHES ON CLUSTER {cluster_name} {table_name}")

    for node in nodes:
        for _ in range(total_parts // 2):
            node.query(
                f"""
                INSERT INTO {table_name}
                SELECT number, toString(number) FROM numbers_mt({table_size / total_parts})
                SETTINGS insert_deduplicate = 0
                """
            )


def _create_query(query_tmpl, table_name):
    return query_tmpl.format(table_name=table_name)


def _get_result_with_parallel_replicas(
    query, cluster_name, parallel_replicas_mark_segment_size
):
    return nodes[0].query(
        query,
        settings={
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": len(nodes),
            "cluster_for_parallel_replicas": f"{cluster_name}",
            "parallel_replicas_mark_segment_size": parallel_replicas_mark_segment_size,
        },
    )


@pytest.mark.parametrize(
    "query_tmpl",
    [
        "SELECT sum(key) FROM {table_name}",
    ],
)
@pytest.mark.parametrize(
    "table_size",
    [1000, 10000, 100000],
)
@pytest.mark.parametrize(
    "index_granularity",
    [11, 101],
)
@pytest.mark.parametrize(
    "parallel_replicas_mark_segment_size",
    [1, 11],
)
def test_reading_with_invisible_parts(
    start_cluster,
    query_tmpl,
    table_size,
    index_granularity,
    parallel_replicas_mark_segment_size,
):
    table_name = f"tbl_{len(query_tmpl)}_{cluster_name}_{table_size}_{index_granularity}_{parallel_replicas_mark_segment_size}"
    _create_tables(table_name, table_size, index_granularity)

    query = _create_query(query_tmpl, table_name)

    assert table_size % total_parts == 0
    rows_per_part = table_size // total_parts
    expected = total_parts * ((rows_per_part * (rows_per_part - 1)) // 2)
    assert (
        _get_result_with_parallel_replicas(
            query, cluster_name, parallel_replicas_mark_segment_size
        )
        == f"{expected}\n"
    )
    nodes[0].query(f"DROP TABLE {table_name} ON CLUSTER {cluster_name} SYNC")
