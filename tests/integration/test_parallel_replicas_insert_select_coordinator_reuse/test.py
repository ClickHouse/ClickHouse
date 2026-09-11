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


def test_insert_select_coordinator_reuse_with_drifting_liveness(start_cluster):
    # Regression test for the INSERT SELECT coordinator-reuse path.
    #
    # An INSERT SELECT with parallel replicas builds the local SELECT pipeline first - this creates the
    # reading coordinator and sizes it from a liveness snapshot (`is_active`, see `system.clusters`) - and
    # only then builds the remote pipelines. The remote-pool pass must reuse the exact connection pools and
    # replica numbering decided for that coordinator, not recompute liveness from a fresh snapshot: if a
    # replica's liveness changed in between, a recomputed (larger) active set would assign a replica number
    # that is out of range for the already-sized coordinator, and the coordinator would reject the
    # announcement with `Replica number (N) is bigger than total replicas count (M)`.
    #
    # That race cannot be reproduced deterministically from outside, so the failpoint
    # `parallel_replicas_insert_select_drop_active_replica` (ONCE) drops one active non-local replica from the
    # coordinator-building snapshot only. (The throwaway suitability probe that runs first, just to detect
    # whether the SELECT reads with parallel replicas, is excluded from the failpoint on the server side, so
    # the ONCE drop lands on the executed coordinator and not on the discarded probe plan.) The coordinator is
    # therefore sized for 2 replicas while all 3 are actually active; the second pass, if it recomputed
    # liveness, would see 3 again. With the fix the INSERT SELECT succeeds and inserts every row; without it the
    # coordinator rejects the out-of-range replica.
    db = "pr_db_insert_select"
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/databases/{db}', 'shard1', 'replica{i}')"
        )

    node1.query(
        f"CREATE TABLE {db}.src (key Int64, value String) ENGINE = ReplicatedMergeTree ORDER BY key"
    )
    node1.query(
        f"CREATE TABLE {db}.dst (key Int64, value String) ENGINE = ReplicatedMergeTree ORDER BY key"
    )
    node1.query(
        f"INSERT INTO {db}.src SELECT number, toString(number) FROM numbers(100000)"
    )
    # Every replica must hold a full local copy of the source so any subset of replicas covers all the data.
    node2.query(f"SYSTEM SYNC REPLICA {db}.src")
    node3.query(f"SYSTEM SYNC REPLICA {db}.src")

    # All three replicas are registered and active - the failpoint, not a stopped node, creates the drift.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
        "3\n",
    )

    settings = {
        "enable_parallel_replicas": 1,
        "max_parallel_replicas": 3,
        "cluster_for_parallel_replicas": db,
        "parallel_distributed_insert_select": 2,
        # Keep the local-pipeline INSERT SELECT path (where the coordinator is reused) active, independently
        # of any future change to the defaults.
        "parallel_replicas_local_plan": 1,
        "parallel_replicas_insert_select_local_pipeline": 1,
        "parallel_replicas_prefer_local_replica": 1,
    }

    node1.query(
        "SYSTEM ENABLE FAILPOINT parallel_replicas_insert_select_drop_active_replica"
    )
    try:
        node1.query(f"INSERT INTO {db}.dst SELECT * FROM {db}.src", settings=settings)

        # Remote replicas insert their portion into their local (replicated) `dst`; pull those parts in before
        # counting so the assertion sees the whole result regardless of replication lag.
        node1.query(f"SYSTEM SYNC REPLICA {db}.dst")

        # The reused coordinator stayed consistent: every source row was inserted exactly once.
        assert node1.query(f"SELECT count() FROM {db}.dst") == "100000\n"
        assert (
            node1.query(
                f"SELECT * FROM {db}.dst ORDER BY key EXCEPT SELECT * FROM {db}.src ORDER BY key"
            )
            == ""
        )

        # The coordinator was sized for 2 replicas (the failpoint dropped one from the first snapshot); the
        # remote pass reused that set rather than the 3 replicas a fresh liveness read would have reported.
        assert node1.contains_in_log(
            "Creating parallel replicas coordinator with replicas_count=2"
        )
    finally:
        node1.query(
            "SYSTEM DISABLE FAILPOINT parallel_replicas_insert_select_drop_active_replica"
        )
