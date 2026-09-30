import pytest

from helpers.cluster import ClickHouseCluster

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


def test_configured_cluster_shadowing_replicated_db(start_cluster):
    # Regression test for the case where `cluster_for_parallel_replicas` names a configured cluster that shares
    # its name with a `Replicated` database. `Context::getCluster` resolves the configured cluster first, so the
    # parallel-replicas cluster is the configured one - not the database's own cluster.
    #
    # The active-replica sizing must consult liveness only when the resolved cluster really is the database's own
    # cluster. Otherwise it would feed the configured cluster (whose replica names do not match the database's
    # ZooKeeper nodes) into `DatabaseReplicated::tryGetReplicasInfo`, which then reports every replica inactive.
    # That would collapse the coordinator onto the single local replica even though all three replicas are online.
    #
    # A configured cluster carries no liveness signal (its `is_active` is unknown, just like in `system.clusters`),
    # so the coordinator must fall back to using all replicas.
    db = "shadow_db"  # same name as the configured cluster in configs/config.xml
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
    # Make sure every replica holds a full local copy of the data.
    node2.query(f"SYSTEM SYNC REPLICA {db}.tt")
    node3.query(f"SYSTEM SYNC REPLICA {db}.tt")

    # All three replicas of the same-named configured cluster are online (one `system.one` row per replica).
    assert (
        node1.query(f"SELECT count() FROM clusterAllReplicas('{db}', system.one)")
        == "3\n"
    )

    # Run a data-reading query with parallel replicas over the configured cluster (a trivial `count()` is answered
    # from metadata and would never engage parallel replicas, so the coordinator would not be created at all).
    result = node1.query(
        f"SELECT sum(key) FROM {db}.tt",
        settings={
            "enable_parallel_replicas": 1,
            "max_parallel_replicas": 3,
            "cluster_for_parallel_replicas": db,
        },
    )
    assert result == "4999950000\n"

    # The configured cluster has no known liveness, so all three replicas must be used - the coordinator must not
    # collapse onto the single local replica by misreading the database's liveness through the wrong cluster.
    assert node1.contains_in_log(
        "Creating parallel replicas coordinator with replicas_count=3"
    )
    assert not node1.contains_in_log(
        "Creating parallel replicas coordinator with replicas_count=1"
    )
