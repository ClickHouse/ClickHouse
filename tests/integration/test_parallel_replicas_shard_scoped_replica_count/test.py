import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3, 4, 5)
]
# `test_asymmetric_shards`: shard 1 = {n1, n2}, shard 2 = {n3, n4, n5}.
shard1_nodes = nodes[:2]
shard2_nodes = nodes[2:]

CLUSTER = "test_asymmetric_shards"
TABLE = "tt"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replica_count_is_scoped_to_the_shard(start_cluster):
    # A parallel-replicas read over a `Distributed` table is dispatched per shard: every shard gets its own
    # reading coordinator, sized by that shard's replica set (`prepareClusterForParallelReplicas` narrows the
    # cluster by the per-query `_shard_num` scalar). The mark-segment-size heuristic must be scoped the same
    # way, otherwise shard 2 is sized by shard 1's replica count -- either by reading shard 0 of the
    # unnarrowed cluster, or by inheriting the count propagated for the previous shard through `ClientInfo`.
    # The two shards here therefore have deliberately different replica counts: 2 and 3.
    for i, node in enumerate(shard1_nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64, value String) "
            f"ENGINE = ReplicatedMergeTree('/test/shard_scoped/shard1/{TABLE}', 'r{i}') ORDER BY key"
        )
    for i, node in enumerate(shard2_nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64, value String) "
            f"ENGINE = ReplicatedMergeTree('/test/shard_scoped/shard2/{TABLE}', 'r{i}') ORDER BY key"
        )

    nodes[0].query(
        f"CREATE TABLE {TABLE}_d AS {TABLE} ENGINE = Distributed({CLUSTER}, currentDatabase(), {TABLE}, key)"
    )
    nodes[0].query(
        f"INSERT INTO {TABLE}_d SELECT number, toString(number) FROM numbers(100000)",
        settings={"distributed_foreground_insert": 1},
    )
    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA {TABLE}")

    assert (
        nodes[0].query(
            f"SELECT sum(key) FROM {TABLE}_d",
            settings={
                # 2 = force parallel replicas, so the read cannot silently fall back to a plain read and
                # leave the assertions below with nothing to match.
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
            },
        )
        == "4999950000\n"
    )

    # Every node that read data for a shard must have sized the heuristic by that shard's replica count.
    # `number_of_replicas` is logged by `chooseSegmentSize` (see `MergeTreeReadPoolParallelReplicas`).
    assert any(node.contains_in_log("number_of_replicas=2,") for node in shard1_nodes)
    assert any(node.contains_in_log("number_of_replicas=3,") for node in shard2_nodes)

    # And no node may be sized by the *other* shard's count. Shard 2 seeing 2 is the regression this guards:
    # it means the shard scope was ignored, either when reading the cluster or when reusing the count the
    # initiator propagated for shard 1.
    for node in shard1_nodes:
        assert not node.contains_in_log("number_of_replicas=3,")
    for node in shard2_nodes:
        assert not node.contains_in_log("number_of_replicas=2,")
