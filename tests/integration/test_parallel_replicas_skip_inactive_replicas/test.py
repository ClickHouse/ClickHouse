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


def test_inactive_replica_excluded_from_parallel_replicas(start_cluster):
    # A `Replicated` database exposes a cluster (named after the database) whose membership is the set of
    # *registered* replicas, while `is_active` (see `system.clusters`) reflects which of them are currently
    # online. This is precisely the situation the bug needs: a registered-but-inactive replica.
    #
    # The reading coordinator distributes mark segments by hashing over the number of replicas. If an inactive
    # replica is counted, its hash buckets become "phantom" segments that only the source replica can pick up,
    # producing a severe work-distribution skew. The coordinator must therefore be sized by the number of
    # *active* replicas, not the number of registered ones.
    db = "pr_db"
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

    # All three replicas are registered and active.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
        "3\n",
    )

    # Stop one replica gracefully: it stays registered in the cluster definition but becomes inactive.
    node3.stop_clickhouse()
    try:
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
            "2\n",
            retry_count=60,
            sleep_time=1,
        )
        # The inactive replica is still part of the cluster definition.
        assert node1.query(f"SELECT count() FROM system.clusters WHERE cluster = '{db}'") == "3\n"

        # Run a query with parallel replicas over the database cluster, requesting more replicas than are
        # online so that the coordinator is sized by what is actually available. Use a data-reading query
        # (not a trivial `count()`, which `optimize_trivial_count_query` answers from metadata and thus never
        # engages parallel replicas, so the coordinator would not be created at all).
        result = node1.query(
            f"SELECT sum(key) FROM {db}.tt",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": db,
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
