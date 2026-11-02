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
# The second old node runs 26.10, which speaks protocol 9 but does not read in order through a `JOIN`.
split_topology_nodes = [
    cluster.add_instance(
        f"split_node{num}",
        main_configs=["configs/clusters_split_topology.xml"],
        with_zookeeper=True,
        image="clickhouse/clickhouse-server",
        tag=tag,
        stay_alive=True,
        with_installed_binary=True,
    )
    for num, tag in enumerate(["26.5", "26.10"])
] + [
    # Third node intentionally uses the current build (no `image`/`tag`/`with_installed_binary`),
    # so the cluster mixes 26.5 and 26.10 with the version under test — exactly the rolling-upgrade shape.
    cluster.add_instance(
        "split_node2",
        main_configs=["configs/clusters_split_topology.xml"],
        with_zookeeper=True,
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
    # No OPTIMIZE FINAL: a single insert is one part, so FINAL is a no-op merge whose MERGE_PARTS entry
    # the old replicas cannot reproduce byte-identically across versions and can hang for minutes. The
    # assertions below do not depend on part layout.
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
    # No OPTIMIZE FINAL: a single insert is one part, so FINAL is a no-op merge whose MERGE_PARTS
    # entry the 26.5 replicas cannot reproduce byte-identically across versions and can hang for
    # minutes, occasionally raising code 341 (log entry not processed, replica shut down). The
    # assertions below do not depend on part layout. system sync replica already guarantees
    # every replica sees the same 1M rows.
    for node in split_topology_nodes:
        node.query("system sync replica ts")

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


def test_read_in_order_through_join_rolling_upgrade(start_cluster):
    # The version under test reads in order through a `LEFT JOIN` with parallel replicas, while 26.5 and
    # 26.10 read such a stream in `Default` mode. Whichever node is the initiator, the replicas taking part
    # must announce the same coordination mode: a newer initiator does not use the older replicas, and
    # a newer follower of an older initiator does not read in order through the join.
    for num in range(len(split_topology_nodes)):
        node = split_topology_nodes[num]
        node.query("drop table if exists tj sync")
        node.query("drop table if exists rj sync")
        node.query(
            f"""
            create table tj(k UInt64, v UInt64)
            engine = ReplicatedMergeTree('/test_backward_compatability/test_parallel_replicas_protocol/split_topology/shard0/tj', '{num}')
            order by (k, v)
        """
        )
        node.query("create table rj(k UInt64, s String) engine = MergeTree order by k")
        node.query("insert into rj select number * 2, toString(number) from numbers(1000)")
    split_topology_nodes[0].query(
        "insert into tj select number % 100000, number from numbers_mt(1000000)"
    )
    for node in split_topology_nodes:
        node.query("system sync replica tj")

    queries = [
        (
            "select tj.k, tj.v, rj.s from tj left join rj on tj.k = rj.k order by tj.k, tj.v limit 3",
            "0\t0\t0\n0\t100000\t0\n0\t200000\t0\n",
        ),
        (
            "select tj.k, count(), any(rj.s) from tj left join rj on tj.k = rj.k group by tj.k order by tj.k limit 3",
            "0\t10\t0\n1\t10\t\n2\t10\t1\n",
        ),
        (
            "select distinct tj.k, rj.s from tj left join rj on tj.k = rj.k order by tj.k limit 3",
            "0\t0\n1\t\n2\t1\n",
        ),
    ]

    for local_plan in (1, 0):
        settings = {
            "enable_analyzer": 1,
            "cluster_for_parallel_replicas": "parallel_replicas",
            "max_parallel_replicas": 3,
            "enable_parallel_replicas": 1,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "parallel_replicas_local_plan": local_plan,
            "optimize_read_in_order": 1,
            "optimize_aggregation_in_order": 1,
            "optimize_distinct_in_order": 1,
            "query_plan_read_in_order_through_join": 1,
            "query_plan_join_swap_table": 0,
            "join_algorithm": "hash",
            # A spilling join does not keep the order of its left input.
            "max_bytes_before_external_join": 0,
            "max_bytes_ratio_before_external_join": 0,
        }
        for node in split_topology_nodes:
            for query, expected in queries:
                # Make every replica take part instead of letting the initiator finish alone.
                node.query("system enable failpoint parallel_replicas_wait_for_unused_replicas")
                assert node.query(query, settings=settings) == expected, (
                    node.name,
                    local_plan,
                    query,
                )

    for node in split_topology_nodes:
        node.query("system disable failpoint parallel_replicas_wait_for_unused_replicas")
        node.query("drop table tj sync")
        node.query("drop table rj sync")
