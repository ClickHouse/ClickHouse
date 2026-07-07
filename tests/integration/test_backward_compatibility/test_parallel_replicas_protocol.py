import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        image="clickhouse/clickhouse-server",
        tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
        stay_alive=True,
        use_old_analyzer=False,
        with_installed_binary=True,
    )
    for num in range(2)
] + [
    cluster.add_instance(
        "node2",
        # node2 runs the new build, which deduplicates inserts with the unified hash only — the old
        # replicas of this shared ReplicatedMergeTree do not share that hash. The table is therefore
        # populated from a single replica and propagated by replication (see the test body) instead
        # of relying on cross-version insert deduplication.
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        use_old_analyzer=False,
    )
]

# Separate cluster for the rolling-upgrade-with-split-topology scenario. 26.5 is the
# first stable release that speaks parallel-replicas protocol 7 (with `stream_id`
# support) but NOT 8 (the new announcement-response packet introduced on this branch).
# That's the narrow window the split-stream topology this branch creates has to
# tolerate: the older initiator raises "more initial requests than there are
# replicas" when a newer follower over-announces, and the newer coordinator's
# snapshot-pin / unknown-stream paths must degrade gracefully when an older follower
# under-announces. Kept separate from the 24.3 cluster above so each test exercises
# exactly one version skew. (Older PR<7 peers are filtered out at connection time by
# `RemoteQueryExecutor`, so they never reach the split-stream code path.)
split_topology_nodes = [
    cluster.add_instance(
        f"split_node{num}",
        main_configs=["configs/clusters_split_topology.xml"],
        with_zookeeper=True,
        image="clickhouse/clickhouse-server",
        tag="26.5",
        stay_alive=True,
        use_old_analyzer=False,
        with_installed_binary=True,
    )
    for num in range(2)
] + [
    # Third node intentionally uses the current build (no `image`/`tag`/`with_installed_binary`),
    # so the cluster mixes 26.5 with the version under test — exactly the rolling-upgrade shape.
    cluster.add_instance(
        "split_node2",
        main_configs=["configs/clusters_split_topology.xml"],
        with_zookeeper=True,
        use_old_analyzer=False,
    )
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability(start_cluster):
    for num in range(len(nodes)):
        node = nodes[num]
        node.query("drop table if exists t sync")
        node.query(
            f"""
            create table if not exists t(a UInt64)
            engine = ReplicatedMergeTree('/test_backward_compatability/test_parallel_replicas_protocol/shard0/t', '{num}')
            order by (a)
        """
        )

    # Populate from a single replica and let replication propagate the data to the others. Inserting
    # the same data from every replica would rely on cross-version insert deduplication, which the
    # new build (unified hash only) no longer shares with the old replicas.
    nodes[0].query("insert into t select number % 100000 from numbers_mt(1000000) ORDER BY ALL")
    nodes[0].query("optimize table t final")
    for node in nodes:
        node.query("system sync replica t")

    # all we want is the query to run without errors
    for node in nodes:
        assert (
            node.query(
                """
                select sum(a)
                from t
                """,
                settings={
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "max_parallel_replicas": 3,
                    "allow_experimental_parallel_reading_from_replicas": 1,
                    "parallel_replicas_for_non_replicated_merge_tree": 1,
                    "merge_tree_min_rows_for_concurrent_read": 0,
                    "merge_tree_min_bytes_for_concurrent_read": 0,
                    "merge_tree_min_read_task_size": 1,
                },
            )
            == "49999500000\n"
        )

    # WithOrder (ORDER BY + read_in_order optimization)
    for node in nodes:
        assert (
            node.query(
                """
                select a
                from t
                order by a
                limit 10
                """,
                settings={
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "max_parallel_replicas": 3,
                    "allow_experimental_parallel_reading_from_replicas": 1,
                    "parallel_replicas_for_non_replicated_merge_tree": 1,
                    "merge_tree_min_rows_for_concurrent_read": 0,
                    "merge_tree_min_bytes_for_concurrent_read": 0,
                    "merge_tree_min_read_task_size": 1,
                    "optimize_read_in_order": 1,
                },
            )
            == "0\n" * 10
        )

    # ReverseOrder (ORDER BY DESC + read_in_order optimization)
    for node in nodes:
        assert (
            node.query(
                """
                select a
                from t
                order by a desc
                limit 10
                """,
                settings={
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "max_parallel_replicas": 3,
                    "allow_experimental_parallel_reading_from_replicas": 1,
                    "parallel_replicas_for_non_replicated_merge_tree": 1,
                    "merge_tree_min_rows_for_concurrent_read": 0,
                    "merge_tree_min_bytes_for_concurrent_read": 0,
                    "merge_tree_min_read_task_size": 1,
                    "optimize_read_in_order": 1,
                },
            )
            == "99999\n" * 10
        )

    for node in nodes:
        node.query("drop table t sync")


def test_split_topology_rolling_upgrade(start_cluster):
    # With `parallel_replicas_local_plan = 1` and `max_threads > 1`, this branch's
    # initiator (and any new follower) splits the in-order read into multiple
    # `#split_i` streams. The 26.5 peer doesn't know about the
    # announcement-response packet that authorises each split — without the
    # version-aware degradation, the older initiator raises
    # `Initiator received more initial requests than there are replicas` when a
    # newer follower over-announces, and the newer coordinator throws on an
    # unknown stream when an older follower under-announces. We exercise both
    # directions by iterating each node as the initiator.
    # Insert from exactly one node and sync the rest so every replica sees the same 1M rows.
    # The count() assertion below is sensitive to the total row count — inserting once per
    # node (as in `test_backward_compatability` above) would leave the replicated table
    # somewhere between 1M and 3M rows depending on replication timing, and parallel-replicas
    # dedupe would still let `sum()`/`limit` queries look correct while `count()` amplifies.
    for num in range(len(split_topology_nodes)):
        node = split_topology_nodes[num]
        node.query("drop table if exists ts sync")
        node.query(
            f"""
            create table if not exists ts(a UInt64)
            engine = ReplicatedMergeTree('/test_backward_compatability/test_parallel_replicas_protocol/split_topology/shard0/ts', '{num}')
            order by (a)
        """
        )
    split_topology_nodes[0].query(
        "insert into ts select number % 100000 from numbers_mt(1000000) ORDER BY ALL"
    )
    for node in split_topology_nodes:
        node.query("system sync replica ts")
        node.query("optimize table ts final")

    split_settings = {
        "cluster_for_parallel_replicas": "parallel_replicas",
        "max_parallel_replicas": 3,
        "allow_experimental_parallel_reading_from_replicas": 1,
        "parallel_replicas_for_non_replicated_merge_tree": 1,
        "merge_tree_min_rows_for_concurrent_read": 0,
        "merge_tree_min_bytes_for_concurrent_read": 0,
        "merge_tree_min_read_task_size": 1,
        "optimize_read_in_order": 1,
        "parallel_replicas_local_plan": 1,
        "max_threads": 4,
    }

    for node in split_topology_nodes:
        assert (
            node.query(
                "select a from ts order by a limit 10",
                settings=split_settings,
            )
            == "0\n" * 10
        )

    for node in split_topology_nodes:
        assert (
            node.query(
                "select a from ts order by a desc limit 10",
                settings=split_settings,
            )
            == "99999\n" * 10
        )

    # Non-idempotent aggregate: catches silent work duplication that the idempotent
    # `LIMIT 10` queries above would mask. Each `a` value appears exactly 10 times
    # (number % 100000 over 1e6 rows); under the split-stream bug a newer follower's
    # per-split pools would each read the same parts and inflate the count by
    # `~max_threads`.
    for node in split_topology_nodes:
        assert (
            node.query(
                """
                select a, count()
                from ts
                group by a
                order by a
                limit 5
                """,
                settings={**split_settings, "optimize_aggregation_in_order": 1},
            )
            == "0\t10\n1\t10\n2\t10\n3\t10\n4\t10\n"
        )

    for node in split_topology_nodes:
        node.query("drop table ts sync")
