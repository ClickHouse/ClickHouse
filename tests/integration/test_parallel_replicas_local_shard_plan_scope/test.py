import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3, 4)
]
# `test_local_shard_plan`: shard 1 = {n1}, shard 2 = {n2, n3, n4}.
shard1_nodes = nodes[:1]
shard2_nodes = nodes[1:]

CLUSTER = "test_local_shard_plan"
TABLE = "tt"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_local_shard_plan_carries_the_shard_scope(start_cluster):
    # `_shard_num` reaches a shard through two carriers. A remote sub-query gets it as a regular scalar sent
    # over the wire, but the initiator's own shard is dispatched as a local plan when the shard has a single
    # replica (`prefer_localhost_replica` branch: a single-replica shard keeps `parallel_replicas_enabled`
    # false for the dispatch, so `createLocalPlan` is used), and there the shard number is injected with
    # `addSpecialScalar` instead. The parallel-replicas shard scoping must read that carrier too: before it
    # did, this query threw `UNEXPECTED_CLUSTER` from the local shard's plan, because a multi-shard
    # `cluster_for_parallel_replicas` with no visible `_shard_num` is rejected.
    for i, node in enumerate(shard1_nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64, value String) "
            f"ENGINE = ReplicatedMergeTree('/test/local_shard_plan/shard1/{TABLE}', 'r{i}') ORDER BY key"
        )
    for i, node in enumerate(shard2_nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64, value String) "
            f"ENGINE = ReplicatedMergeTree('/test/local_shard_plan/shard2/{TABLE}', 'r{i}') ORDER BY key"
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
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                # Set explicitly: the automatic override to the `Distributed` cluster happens only for the
                # remote pipes, so this is what the local shard's plan sees, and it has 2 shards.
                "cluster_for_parallel_replicas": CLUSTER,
                "prefer_localhost_replica": 1,
            },
        )
        == "4999950000\n"
    )

    # Shard 2 runs a shard-scoped parallel-replicas read and must size the mark-segment heuristic by its own
    # replica count (`number_of_replicas` is logged by `chooseSegmentSize`).
    assert any(node.contains_in_log("number_of_replicas=3,") for node in shard2_nodes)

    # The local shard has a single replica: once its plan sees the shard scope from the special scalar, it
    # concludes parallel replicas are not useful there and does a plain local read. Sizing it by any other
    # scope (for example the whole cluster's 4 nodes, or shard 2's count) would mean the scope leaked.
    for node in shard1_nodes:
        assert not node.contains_in_log("number_of_replicas=")
