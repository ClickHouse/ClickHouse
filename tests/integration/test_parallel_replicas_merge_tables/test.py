"""Parallel replicas over Merge tables and the merge() table function.

https://github.com/ClickHouse/ClickHouse/issues/67770

Reading from every underlying MergeTree table of a Merge table is coordinated
across replicas: each underlying table forms its own data stream in the reading
coordinator, so all replicas participate in reading all underlying tables.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas_cluster"

nodes = [
    cluster.add_instance(
        f"node{num}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for num in range(3)
]

parallel_replicas_settings = {
    "enable_analyzer": 1,
    "enable_parallel_replicas": 2,
    "max_parallel_replicas": 3,
    "cluster_for_parallel_replicas": cluster_name,
    "parallel_replicas_allow_merge_tables": 1,
    "automatic_parallel_replicas_mode": 0,
    # Hand out small mark segments so the reading work is spread across all replicas instead of a
    # single (usually local) replica grabbing a whole underlying table at once — otherwise
    # `_assert_all_replicas_participated` is racy on the small test dataset.
    "parallel_replicas_mark_segment_size": 10,
}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        for num, node in enumerate(nodes):
            node.query(
                f"""
                CREATE TABLE tbl_1 (key Int64, value String)
                ENGINE = ReplicatedMergeTree('/test_parallel_replicas_merge/shard1/tbl_1', '{num}')
                ORDER BY (key)
                SETTINGS index_granularity = 10
                """
            )
            # A different structure (narrower key type) to cover per-table conversion.
            node.query(
                f"""
                CREATE TABLE tbl_2 (key Int32, value String)
                ENGINE = ReplicatedMergeTree('/test_parallel_replicas_merge/shard1/tbl_2', '{num}')
                ORDER BY (key)
                SETTINGS index_granularity = 10
                """
            )
            node.query("CREATE TABLE tbl_merge ENGINE = Merge(default, '^tbl_[12]$')")

        nodes[0].query(
            "INSERT INTO tbl_1 SELECT number, toString(number) FROM numbers_mt(100000)"
        )
        nodes[0].query(
            "INSERT INTO tbl_2 SELECT number + 100000, toString(number) FROM numbers_mt(100000)"
        )
        for node in nodes:
            node.query("SYSTEM SYNC REPLICA tbl_1 STRICT")
            node.query("SYSTEM SYNC REPLICA tbl_2 STRICT")

        yield cluster
    finally:
        cluster.shutdown()


def _assert_all_replicas_participated(query_id):
    """Every replica must have executed a secondary query and read some data."""
    for node in nodes:
        node.query("SYSTEM FLUSH LOGS query_log")

    participants = 0
    for node in nodes:
        read_rows = node.query(
            f"""
            SELECT sum(read_rows)
            FROM system.query_log
            WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish' AND NOT is_initial_query
            """
        ).strip()
        if read_rows != "" and int(read_rows) > 0:
            participants += 1

    # The initiator participates through its local plan (not a secondary query),
    # so at least the two remote replicas must have read something.
    assert participants >= 2


@pytest.mark.parametrize(
    "table_expression", ["tbl_merge", "merge(default, '^tbl_[12]$')"]
)
def test_aggregation_over_merge_table(start_cluster, table_expression):
    query = f"SELECT count(), sum(key) FROM {table_expression} WHERE key % 3 = 0"
    expected = nodes[0].query(query, settings={"enable_parallel_replicas": 0})

    query_id = f"pr_merge_agg_{len(table_expression)}"
    assert (
        nodes[0].query(query, settings={**parallel_replicas_settings, "query_id": query_id})
        == expected
    )
    _assert_all_replicas_participated(query_id)


def test_non_aggregate_query(start_cluster):
    query = "SELECT key, value FROM tbl_merge WHERE key % 20000 = 7 ORDER BY key"
    expected = nodes[0].query(query, settings={"enable_parallel_replicas": 0})

    query_id = "pr_merge_plain"
    assert (
        nodes[0].query(query, settings={**parallel_replicas_settings, "query_id": query_id})
        == expected
    )
    _assert_all_replicas_participated(query_id)


def test_filter_by_table_virtual_column(start_cluster):
    query = "SELECT count(), sum(key) FROM tbl_merge WHERE _table = 'tbl_2'"
    expected = nodes[0].query(query, settings={"enable_parallel_replicas": 0})

    assert (
        nodes[0].query(
            query, settings={**parallel_replicas_settings, "query_id": "pr_merge_virt"}
        )
        == expected
    )


def test_group_by(start_cluster):
    query = (
        "SELECT key % 5 AS g, count(), sum(key), uniqExact(value) FROM tbl_merge"
        " GROUP BY g ORDER BY g"
    )
    expected = nodes[0].query(query, settings={"enable_parallel_replicas": 0})

    query_id = "pr_merge_group_by"
    assert (
        nodes[0].query(query, settings={**parallel_replicas_settings, "query_id": query_id})
        == expected
    )
    _assert_all_replicas_participated(query_id)


def test_setting_disabled(start_cluster):
    """Without the setting the query must not be distributed."""
    query_id = "pr_merge_disabled"
    nodes[0].query(
        "SELECT count() FROM tbl_merge",
        settings={
            **parallel_replicas_settings,
            "parallel_replicas_allow_merge_tables": 0,
            "query_id": query_id,
        },
    )
    for node in nodes:
        node.query("SYSTEM FLUSH LOGS query_log")
    for node in nodes[1:]:
        assert (
            node.query(
                f"""
                SELECT count()
                FROM system.query_log
                WHERE initial_query_id = '{query_id}' AND NOT is_initial_query
                """
            ).strip()
            == "0"
        )
