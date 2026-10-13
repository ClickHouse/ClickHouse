import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"r{i}"},
    )
    for i in range(1, 5)
]

CLUSTER = "test_single_shard_multiple_replicas"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_and_fill(table_name):
    nodes[0].query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER {CLUSTER} SYNC")
    nodes[0].query(
        f"""
        CREATE TABLE {table_name} ON CLUSTER {CLUSTER}
        (
            key UInt32,
            x UInt8,
            a1 String ALIAS toString(x),
            a2 String ALIAS toString(x)
        )
        Engine=MergeTree ORDER BY key
        """
    )
    # Non-replicated MergeTree + parallel replicas: every replica needs the full data.
    for node in nodes:
        node.query(
            f"INSERT INTO {table_name} SELECT number, number % 4 FROM numbers(1000)"
        )


# Two ALIAS columns expanding to the same expression (toString(x)) referenced in GROUP BY used to
# throw NUMBER_OF_COLUMNS_DOESNT_MATCH under parallel replicas, because the duplicate keys collapsed
# by name on the replicas while the initiator expected two columns.
@pytest.mark.parametrize(
    "parallel_replicas_mode,extra_settings",
    [
        ("read_tasks", {}),
        ("custom_key_sampling", {"parallel_replicas_custom_key": "key"}),
        ("custom_key_range", {"parallel_replicas_custom_key": "key"}),
    ],
)
def test_duplicate_alias_columns_group_by(
    start_cluster, parallel_replicas_mode, extra_settings
):
    table_name = "alias_dedup_mt"
    create_and_fill(table_name)

    settings = {
        "max_parallel_replicas": 4,
        "enable_parallel_replicas": 1,
        "parallel_replicas_mode": parallel_replicas_mode,
        "parallel_replicas_for_non_replicated_merge_tree": 1,
        "cluster_for_parallel_replicas": CLUSTER,
        "prefer_localhost_replica": 0,
        "serialize_query_plan": 0,
        "enable_analyzer": 1,
        **extra_settings,
    }

    expected = "".join(f"{i}\t{i}\t250\n" for i in range(4))

    assert (
        nodes[0].query(
            f"SELECT a1, a2, count() FROM {table_name} GROUP BY a1, a2 ORDER BY a1",
            settings=settings,
        )
        == expected
    )

    # Also exercise ORDER BY / HAVING by the duplicate alias.
    assert (
        nodes[0].query(
            f"SELECT a1, a2 FROM {table_name} GROUP BY a1, a2 HAVING a2 = '1' ORDER BY a1",
            settings=settings,
        )
        == "1\t1\n"
    )
