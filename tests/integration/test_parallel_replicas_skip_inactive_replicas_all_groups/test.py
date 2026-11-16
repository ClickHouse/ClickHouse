import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=["configs/config.xml"],
        macros={"shard": 1, "replica": i},
        with_zookeeper=True,
        stay_alive=True,
    )
    for i in (1, 2, 3)
]
node1, node2, node3 = nodes


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_inactive_replica_excluded_for_all_groups_cluster(start_cluster):
    # Same scenario as `test_parallel_replicas_skip_inactive_replicas`, but the query targets the
    # `all_groups.<db>` cluster. Because the replicas are configured into a named replica group, the
    # `Replicated` database exposes both a `<db>` cluster and an `all_groups.<db>` cluster in `system.clusters`.
    #
    # `all_groups.<db>` resolves to the same database only after stripping the `all_groups.` prefix (see
    # `tryGetReplicatedDatabaseCluster`). If the coordinator does not strip the prefix when looking up the
    # liveness data, it gets none and falls back to counting inactive replicas - the very skew this fixes.
    db = "pr_db_all_groups"
    cluster_name = f"all_groups.{db}"
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/databases/{db}', 'shard1', 'replica{i}')"
        )

    node1.query(
        f"CREATE TABLE {db}.tt (key Int64, value String) ENGINE = ReplicatedMergeTree ORDER BY key"
    )
    node1.query(
        f"INSERT INTO {db}.tt SELECT number, toString(number) FROM numbers(100000)"
    )
    # Make sure the surviving replica holds a full local copy of the data.
    node2.query(f"SYSTEM SYNC REPLICA {db}.tt")

    # All three replicas are registered and active in the all-groups cluster.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}' AND is_active = 1",
        "3\n",
    )

    # Stop one replica gracefully: it stays registered in the cluster definition but becomes inactive.
    node3.stop_clickhouse()
    try:
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}' AND is_active = 1",
            "2\n",
            retry_count=60,
            sleep_time=1,
        )
        # The inactive replica is still part of the cluster definition.
        assert (
            node1.query(f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}'")
            == "3\n"
        )

        # Run a query with parallel replicas over the all-groups cluster, requesting more replicas than are
        # online so that the coordinator is sized by what is actually available. Use a data-reading query
        # (not a trivial `count()`, which `optimize_trivial_count_query` answers from metadata and thus never
        # engages parallel replicas, so the coordinator would not be created at all).
        result = node1.query(
            f"SELECT sum(key) FROM {db}.tt",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": cluster_name,
            },
        )
        assert result == "4999950000\n"

        # The coordinator must be created for the 2 active replicas, not the 3 registered ones.
        assert node1.contains_in_log(
            "Creating parallel replicas coordinator with replicas_count=2"
        )
        assert not node1.contains_in_log(
            "Creating parallel replicas coordinator with replicas_count=3"
        )
    finally:
        node3.start_clickhouse()
