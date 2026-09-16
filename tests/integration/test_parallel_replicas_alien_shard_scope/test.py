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
PR_CLUSTER_SAME_SHARD_COUNT = "test_alien_pr_cluster_same_shard_count"
TABLE = "tt"


SHARD1_NODES = nodes[1:]  # fan-out shard 1: n2, n3, n4
SHARD2_NODE = nodes[0]  # fan-out shard 2: n1, the initiator; dispatched as a local plan
SHARD1_SUM = sum(range(1000))
SHARD2_SUM = sum(range(1000, 2000))


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        # The two fan-out shards hold DIFFERENT data, so reading the initiator's shard from any other
        # replica set is visible in the result instead of being masked by identical replicated data.
        for i, node in enumerate(SHARD1_NODES, start=1):
            node.query(
                f"CREATE TABLE {TABLE} (key Int64) "
                f"ENGINE = ReplicatedMergeTree('/test/alien_shard_scope/shard1/{TABLE}', 'r{i}') ORDER BY key"
            )
        SHARD2_NODE.query(
            f"CREATE TABLE {TABLE} (key Int64) "
            f"ENGINE = ReplicatedMergeTree('/test/alien_shard_scope/shard2/{TABLE}', 'r1') ORDER BY key"
        )
        SHARD2_NODE.query(
            f"CREATE TABLE {TABLE}_d AS {TABLE} ENGINE = Distributed({FANOUT_CLUSTER}, currentDatabase(), {TABLE}, key)"
        )
        SHARD1_NODES[0].query(f"INSERT INTO {TABLE} SELECT number FROM numbers(1000)")
        for node in SHARD1_NODES[1:]:
            node.query(f"SYSTEM SYNC REPLICA {TABLE}")
        SHARD2_NODE.query(f"INSERT INTO {TABLE} SELECT number FROM numbers(1000, 1000)")
        yield cluster
    finally:
        cluster.shutdown()


# A parallel-replicas read scopes itself to a shard using the `_shard_num` / `_shard_count` pair the
# initiator propagates. That pair describes the cluster of the `Distributed` dispatch that produced it and
# carries no cluster identity, so the dispatch pins `cluster_for_parallel_replicas` to its own cluster for
# every shard it produces - the remote pipes and the local shard plans alike. A local shard plan that kept
# the user-supplied `cluster_for_parallel_replicas` would apply the fan-out's shard scope to an unrelated
# cluster.
#
# The fan-out's single-replica shard - the one dispatched as a local plan, so the shard number travels as
# a special scalar - is shard 2, and it holds data of its own, so the query has exactly one right answer.
@pytest.mark.parametrize(
    "pr_cluster",
    [
        # 1 shard: applying the alien `_shard_num = 2` here made `prepareClusterForParallelReplicas` throw
        # `Shard number is greater than shard count`.
        pytest.param(PR_CLUSTER, id="different_shard_count"),
        # 2 shards, like the fan-out, but its shard 2 is a different replica set (`n3`, `n4`): the pair is
        # indistinguishable from this cluster's own by the numbers alone, and applying it here made the
        # initiator's shard read that replica set - shard 1's data - instead of its own.
        pytest.param(PR_CLUSTER_SAME_SHARD_COUNT, id="same_shard_count"),
    ],
)
def test_shard_scope_of_another_cluster_is_ignored(start_cluster, pr_cluster):
    assert (
        SHARD2_NODE.query(
            f"SELECT sum(key) FROM {TABLE}_d",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": pr_cluster,
                "prefer_localhost_replica": 1,
            },
        )
        == f"{SHARD1_SUM + SHARD2_SUM}\n"
    )
