import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

initiator = cluster.add_instance(
    "n1", main_configs=["configs/remote_servers_initiator.xml"], with_zookeeper=True
)
# `n4` only exists in the workers' cluster definition; it never holds the table and is never contacted.
workers = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers_workers.xml"], with_zookeeper=True
    )
    for i in (2, 3, 4)
]
data_nodes = [initiator] + workers[:2]

CLUSTER = "pr_plan_based_asym"
TABLE = "tt"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_plan_based_workers_use_the_initiator_replica_count(start_cluster):
    # `createParallelReplicasPlan` (the plan-based dispatch) must propagate the coordinator's replica count to
    # the workers, exactly like the classic `executeQueryWithParallelReplicas` path does. Without that
    # propagation each worker sizes the mark-segment-size heuristic from its *own* view of the cluster, which
    # can differ from the count the reading coordinator was sized with - and then the two halves of
    # `chooseSegmentSize` disagree across nodes, which is the divergence this PR removes.
    #
    # The disagreement is made deterministic by an asymmetric cluster definition (as during a rolling
    # configuration rollout): the initiator sees 3 replicas, the workers see 4. With `max_parallel_replicas = 4`
    # the coordinator is sized by the initiator's 3, while a worker recomputing locally would get 4.
    for i, node in enumerate(data_nodes, start=1):
        node.query(
            f"CREATE TABLE {TABLE} (key Int64, value String) "
            f"ENGINE = ReplicatedMergeTree('/test/plan_based_replica_count/{TABLE}', 'r{i}') ORDER BY key"
        )

    initiator.query(f"INSERT INTO {TABLE} SELECT number, toString(number) FROM numbers(100000)")
    for node in data_nodes[1:]:
        node.query(f"SYSTEM SYNC REPLICA {TABLE}")

    assert (
        initiator.query(
            f"SELECT sum(key) FROM {TABLE}",
            settings={
                "enable_analyzer": 1,
                # 2 = force parallel replicas, so the read cannot silently fall back to a plain read and
                # leave the assertions below with nothing to match.
                "enable_parallel_replicas": 2,
                "parallel_replicas_plan_based": 1,
                # No local plan: every replica that reads is a worker, so the assertions below observe the
                # count the workers were given, not the initiator's own local read.
                "parallel_replicas_local_plan": 0,
                # Keep the cost decision out of the picture: the plan-based split must engage.
                "automatic_parallel_replicas_mode": 0,
                # Above the initiator's replica count, so its cap (3) and a worker's local recomputation (4)
                # really differ; a cap of 3 would collapse both to 3 and the test would prove nothing.
                "max_parallel_replicas": 4,
                "cluster_for_parallel_replicas": CLUSTER,
            },
        )
        == "4999950000\n"
    )

    # The coordinator is sized by the initiator's own view of the cluster.
    assert initiator.contains_in_log(
        "Creating parallel replicas coordinator with replicas_count=3"
    )
    # Every worker that read data must have been sized by the propagated count (3), not by its own
    # 4-replica cluster definition. The negative assertion is the actual regression guard: dropping the
    # propagation in `createParallelReplicasPlan` makes a worker log `number_of_replicas=4,` here.
    # `number_of_replicas` is logged by `chooseSegmentSize` (see `MergeTreeReadPoolParallelReplicas`).
    assert any(node.contains_in_log("number_of_replicas=3,") for node in workers[:2]), {
        node.name: node.grep_in_log("number_of_replicas") for node in workers[:2]
    }
    for node in data_nodes:
        assert not node.contains_in_log("number_of_replicas=4,"), node.grep_in_log(
            "number_of_replicas"
        )
