import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3, 4)
]

FANOUT_CLUSTER = "test_alien_fanout"
PR_CLUSTER = "test_alien_pr_cluster"
TABLE = "tt"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shard_scope_of_another_cluster_is_ignored(start_cluster):
    # A parallel-replicas read scopes itself to a shard using the `_shard_num` / `_shard_count` pair the
    # initiator propagates. That pair describes the cluster of the `Distributed` dispatch that shipped it,
    # which is not necessarily the cluster `cluster_for_parallel_replicas` names: a read nested in a fan-out
    # over another cluster keeps its own parallel-replicas cluster (the automatic override to the
    # `Distributed` cluster happens only for the remote pipes, not for the local shard's plan).
    #
    # Here the fan-out has 2 shards while the parallel-replicas cluster has 1, and the fan-out's
    # single-replica shard - the one dispatched as a local plan, so the shard number travels as a special
    # scalar - is shard 2. Applying that alien scope to the one-shard cluster made
    # `prepareClusterForParallelReplicas` throw `Shard number is greater than shard count`.
    #
    # Every node replicates the same table, so the result does not depend on which replica set is read and
    # the test asserts only that the query runs and returns the right answer.
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64) "
            f"ENGINE = ReplicatedMergeTree('/test/alien_shard_scope/{TABLE}', 'r{i}') ORDER BY key"
        )
    nodes[0].query(
        f"CREATE TABLE {TABLE}_d AS {TABLE} ENGINE = Distributed({FANOUT_CLUSTER}, currentDatabase(), {TABLE}, key)"
    )
    nodes[0].query(f"INSERT INTO {TABLE} SELECT number FROM numbers(1000)")
    for node in nodes[1:]:
        node.query(f"SYSTEM SYNC REPLICA {TABLE}")

    # Both shards of the fan-out read the same replicated data, so the sum is doubled.
    assert (
        nodes[0].query(
            f"SELECT sum(key) FROM {TABLE}_d",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": PR_CLUSTER,
                "prefer_localhost_replica": 1,
            },
        )
        == "999000\n"
    )
