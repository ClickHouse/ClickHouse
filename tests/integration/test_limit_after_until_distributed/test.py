import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/test_cluster.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/test_cluster.xml"])


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        for node in (node1, node2):
            node.query("DROP TABLE IF EXISTS dist SYNC")
            node.query("DROP TABLE IF EXISTS local SYNC")


# The no-count forms `LIMIT AFTER ...` / `LIMIT UNTIL ...` leave `hasLimit()` / `limitLength()` false,
# so `StorageDistributed::getOptimizedQueryProcessingStage` could pick `Complete` for a simple
# `SELECT ... GROUP BY sharding_key` with no `ORDER BY`. That makes every shard apply the range
# independently instead of applying it once on the initiator over the merged stream.
#
# A result-based assertion is not possible here: reaching this gate requires no `ORDER BY`, under which
# `AFTER`/`UNTIL` is stream-position dependent and therefore not deterministic. Instead we assert the
# stage decision directly: the shard whose rows do not pass the boundary must still return all of its
# rows to the initiator (partial stage). Under the buggy `Complete` stage that shard would apply the
# range locally and return zero rows.
@pytest.mark.parametrize(
    "boundary, log_comment",
    [
        ("LIMIT AFTER number >= 10", "limit_after_distributed_after"),
        ("LIMIT UNTIL number >= 0", "limit_after_distributed_until"),
    ],
)
def test_no_count_range_is_applied_on_initiator(boundary, log_comment):
    for node in (node1, node2):
        node.query(
            "CREATE TABLE local (number UInt64) ENGINE = MergeTree() ORDER BY number"
        )
    node1.query(
        "CREATE TABLE dist (number UInt64) "
        "ENGINE = Distributed('test_cluster', currentDatabase(), local, number)"
    )

    # Disjoint data, one value range per shard.
    node1.query("INSERT INTO local SELECT number + 10 FROM numbers(10)")  # 10..19
    node2.query("INSERT INTO local SELECT number FROM numbers(10)")  # 0..9

    settings = {
        "allow_experimental_limit_after": 1,
        "optimize_skip_unused_shards": 1,
        "optimize_distributed_group_by_sharding_key": 1,
        # Keep the range out of any limit pushdown so the shard returns the full mergeable state.
        "distributed_push_down_limit": 0,
        "log_comment": log_comment,
    }

    node1.query(
        f"SELECT number FROM dist GROUP BY number {boundary}",
        settings=settings,
    )

    node2.query("SYSTEM FLUSH LOGS")

    # node2 holds 0..9; for `AFTER number >= 10` none of its rows match, and for `UNTIL number >= 0`
    # all of them match (so a per-shard UNTIL would emit nothing). Either way, applying the range on
    # the initiator means node2 still returns all 10 of its groups in its secondary query.
    result_rows = node2.query(
        "SELECT result_rows FROM system.query_log "
        "WHERE type = 'QueryFinish' AND is_initial_query = 0 "
        f"AND log_comment = '{log_comment}' AND query ILIKE '%group by%' "
        "ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()

    assert (
        result_rows == "10"
    ), f"{boundary}: shard with non-passing rows returned {result_rows!r} rows instead of 10 (range applied per shard?)"
