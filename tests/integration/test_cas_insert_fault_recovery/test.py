import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Two replicas of one ReplicatedMergeTree on a SHARED content-addressed pool.
STORAGE_POLICY = "cas_shared"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node1.xml"],
        macros={"replica": "node1"},
        with_rustfs=True,
        with_zookeeper=True,
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node2.xml"],
        macros={"replica": "node2"},
        with_rustfs=True,
        with_zookeeper=True,
        stay_alive=True,
    )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _wait_until(predicate, timeout=180, interval=2, desc=""):
    # Condition-based wait (systematic-debugging): the ordinary lost-part recovery is asynchronous
    # (part-check retry/backoff), so gating on a fixed-timeout `SYSTEM SYNC REPLICA` is inherently flaky —
    # that call blocks on the very recovery we are waiting for. Poll the actual OUTCOME instead, with a
    # generous cap. Transient errors while node1 is mid-restart are swallowed and retried.
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
        except Exception as e:  # node briefly unavailable during restart, etc.
            last = e
        if last is True:
            return
        time.sleep(interval)
    raise AssertionError("timed out after {}s waiting for: {} (last={!r})".format(timeout, desc, last))


def test_post_multi_termination_uses_ordinary_lost_part_recovery(start_cluster):
    # HISTORY: this test was authored (2026-07-16) against the OLD commit ordering, where the disk
    # commit ran AFTER the Keeper multi — the failpoint then left a phantom ZK part entry and the
    # assertion was "ordinary lost-part recovery runs (ReplicatedDataLoss bumps, empty cover)".
    # One day later the R3 acked-data-loss fix (`77484196b0d`) deliberately REVERSED that order:
    # `renameParts` closes the part's disk-storage transaction BEFORE the Keeper multi, so a part
    # must be durable before its block_id/part znode is registered. Under the new ordering the
    # failpoint (`disk_object_storage_fail_commit_metadata_transaction`, fired from inside
    # `renameParts`) aborts the INSERT BEFORE anything reaches ZK — there is no phantom part, no
    # lost part, and NOTHING to recover. The old predicate waited forever (600s timeouts on all
    # three sanitizer CI lanes of PR#2073 and on a local release build).
    #
    # The test now asserts the NEW invariant, which is strictly stronger for the user:
    #   1. the failed INSERT leaves NO trace: no ZK part entry, no replication-queue debris,
    #      count() stays 0 on both replicas after a node1 restart, and `ReplicatedDataLoss` does
    #      NOT bump (nothing was ever lost);
    #   2. THE R3 GUARD: retrying the SAME insert (same bytes => same block_id) actually lands —
    #      a phantom block_id surviving the failed attempt would silently dedup the retry away
    #      (the acked-data-loss class the reordering exists to prevent);
    #   3. no CA-specific wedge: no LOGICAL_ERROR in either server's log, queues drained.
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS t SYNC")
    node2.query("DROP TABLE IF EXISTS t SYNC")

    create = (
        "CREATE TABLE t (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '{{replica}}') "
        "ORDER BY a SETTINGS storage_policy = '{policy}'"
    ).format(policy=STORAGE_POLICY)
    node1.query(create)
    node2.query(create)

    def loss_count():
        return int(
            node1.query(
                "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedDataLoss'"
            )
            or 0
        )

    loss_before = loss_count()

    # Force the disk commit to throw. Under the R3 ordering this fires inside `renameParts`,
    # BEFORE the Keeper multi — the INSERT fails with nothing registered anywhere (ONCE failpoint).
    node1.query("SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction")
    node1.query_and_get_error("INSERT INTO t VALUES (1)")

    # No phantom state may exist even across a restart: ZK has no part entry, so startup's
    # `checkPartsImpl` has nothing to reconcile and no recovery runs.
    node1.restart_clickhouse()

    def node1_clean():
        # The failed INSERT left no trace: nothing to recover (ReplicatedDataLoss unchanged),
        # no rows, no replication-queue debris.
        cnt = node1.query("SELECT count() FROM t").strip()
        queue = node1.query(
            "SELECT count() FROM system.replication_queue WHERE table = 't'"
        ).strip()
        return loss_count() == loss_before and cnt == "0" and queue == "0"

    _wait_until(
        node1_clean,
        timeout=120,
        desc="node1 restarts clean: no phantom part, no recovery triggered, queue empty",
    )

    # THE R3 GUARD (acked-data-loss class): retrying the SAME insert (same bytes => same block_id)
    # must genuinely land. If the failed attempt had leaked its block_id into ZK, dedup would
    # silently swallow this retry and count() would stay 0 — exactly the silent loss the
    # renameParts-before-Keeper ordering exists to prevent.
    node1.query("INSERT INTO t VALUES (1)")

    def retry_landed_everywhere():
        return (
            node1.query("SELECT count() FROM t").strip() == "1"
            and node2.query("SELECT count() FROM t").strip() == "1"
        )

    _wait_until(
        retry_landed_everywhere,
        timeout=120,
        desc="the retried identical INSERT lands and replicates (no phantom-block_id dedup)",
    )

    # The regression guard: no CA-specific exception / LOGICAL_ERROR left either server wedged. The
    # expected `FILE_DOESNT_EXIST` interserver miss is tolerated (it is not a LOGICAL_ERROR).
    for node in (node1, node2):
        assert not node.contains_in_log(
            "LOGICAL_ERROR"
        ), "unexpected LOGICAL_ERROR in {}'s log — a CA-specific failure, not ordinary lost-part recovery".format(
            node.name
        )

    # Server is healthy (no wedge): a fresh, different INSERT also succeeds end to end.
    node1.query("INSERT INTO t VALUES (2)")

    def replicated_two_rows():
        return (
            node1.query("SELECT count() FROM t").strip() == "2"
            and node2.query("SELECT count() FROM t").strip() == "2"
        )

    _wait_until(replicated_two_rows, timeout=120, desc="fresh INSERT replicates to both replicas")

    node1.query("DROP TABLE IF EXISTS t SYNC")
    node2.query("DROP TABLE IF EXISTS t SYNC")
