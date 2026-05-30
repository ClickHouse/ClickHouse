import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    user_configs=["users.d/cache_on_write.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def sum_dim(metric):
    return float(node.query(
        f"SELECT toFloat64(coalesce(sum(value), 0)) "
        f"FROM system.dimensional_metrics WHERE metric = '{metric}'"
    ).strip())


def sum_hist(metric):
    """`system.histogram_metrics` exposes Prometheus-cumulative bucket rows
    keyed by `le`; the `+Inf` row carries the total observation count for
    each label-set. Sum across label-sets to get the family total."""
    return int(node.query(
        f"SELECT toUInt64(coalesce(sum(value), 0)) "
        f"FROM system.histogram_metrics "
        f"WHERE metric = '{metric}' AND labels['le'] = '+Inf'"
    ).strip())


def test_filesystem_cache_eviction_metrics(start_cluster):
    """
    Verify that filesystem_cache_* eviction metrics are populated when the
    setting is configured globally via the default user profile (configs/users.xml).

    Evictions are driven by background read threads that cannot carry a per-query
    context, so they rely on the global context which reflects the default profile.
    """
    node.query("SYSTEM DROP FILESYSTEM CACHE 'cache_with_eviction_metrics'")
    node.query("DROP TABLE IF EXISTS eviction_metrics_test")
    node.query(
        """
        CREATE TABLE eviction_metrics_test (id UInt64, blob String CODEC(NONE))
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'cache_metrics_policy', min_bytes_for_wide_part = 0
        """
    )

    # Three batches of 100 rows × 8 KiB = 2.4 MiB total.
    # With cache_on_write_operations=1 on the disk, each INSERT fills
    # the 100 KiB cache, evicting the previous batch's data.
    for batch in range(3):
        node.query(
            "INSERT INTO eviction_metrics_test "
            "SELECT number + {offset}, repeat('x', 8192) FROM numbers(100)".format(
                offset=batch * 100
            )
        )

    evictions_before = sum_dim("filesystem_cache_evictions_total")
    by_client_before = sum_dim("filesystem_cache_evictions_by_client_total")

    # batch-0 was evicted by later INSERTs; reading it forces cache misses
    # that evict the current occupants (batch-2 data).
    node.query("SELECT sum(length(blob)) FROM eviction_metrics_test WHERE id < 100",
               settings={"enable_filesystem_cache": "1"})

    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical"
    )

    evictions = sum_dim("filesystem_cache_evictions_total") - evictions_before
    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total") > 0
    assert sum_dim("filesystem_cache_evictions_by_client_total") - by_client_before > 0

    slru_queue_evictions = float(node.query(
        "SELECT coalesce(sum(value), 0) FROM system.dimensional_metrics "
        "WHERE metric = 'filesystem_cache_evictions_total' "
        "AND labels['queue'] IN ('probationary', 'protected')"
    ).strip())
    assert slru_queue_evictions > 0, (
        f"No evictions with probationary/protected queue label:\n{debug}"
    )
