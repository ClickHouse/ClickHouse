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


def test_local_replica_kept_when_liveness_reports_it_inactive(start_cluster):
    # Regression test for the "never filter out the local (initiator) replica" safeguard.
    #
    # When the parallel-replicas reading coordinator is sized by `is_active` liveness (see `system.clusters`),
    # the initiator's own `active` znode can be transiently missing - it is recreated on the next ZooKeeper
    # reconnect. The initiator is running the query, so it is online by definition and must never be dropped
    # from the pool; otherwise `findLocalReplicaIndexAndUpdatePools` cannot find it and the query fails with
    # INCONSISTENT_CLUSTER_DEFINITION - a query that used to run starts erroring.
    #
    # That transient ZooKeeper window cannot be reproduced deterministically from outside (the `active`
    # ephemeral is recreated on reconnect), so a failpoint forces the local replica's liveness to `false` on
    # the initiator right before the safeguard runs. The query must still succeed (the safeguard re-activates
    # the local replica), and the coordinator must still be sized for all three active replicas.
    db = "pr_db_local"
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

    # All three replicas are registered and active.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
        "3\n",
    )

    settings = {
        "enable_parallel_replicas": 1,
        "max_parallel_replicas": 3,
        "cluster_for_parallel_replicas": db,
        # Keep the local-plan path that calls `findLocalReplicaIndexAndUpdatePools` (where the regression
        # would surface) active, independently of any future change to the defaults.
        "parallel_replicas_local_plan": 1,
        "parallel_replicas_prefer_local_replica": 1,
    }

    node1.query(
        "SYSTEM ENABLE FAILPOINT parallel_replicas_force_local_replica_inactive"
    )
    try:
        # With the failpoint the initiator's liveness is reported as inactive. The safeguard must keep it, so
        # the query succeeds rather than failing with INCONSISTENT_CLUSTER_DEFINITION. Use a data-reading
        # query (not a trivial `count()`, which `optimize_trivial_count_query` answers from metadata and thus
        # never engages parallel replicas, so the coordinator would not be created at all).
        result = node1.query(f"SELECT sum(key) FROM {db}.tt", settings=settings)
        assert result == "4999950000\n"

        # The local replica was not dropped: the coordinator is still sized for all three active replicas.
        assert node1.contains_in_log(
            "Creating parallel replicas coordinator with replicas_count=3"
        )
    finally:
        node1.query(
            "SYSTEM DISABLE FAILPOINT parallel_replicas_force_local_replica_inactive"
        )
