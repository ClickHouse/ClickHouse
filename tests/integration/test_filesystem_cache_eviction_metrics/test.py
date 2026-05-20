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
    Verify that the `filesystem_cache_*` eviction metrics surface via
    `system.dimensional_metrics` / `system.histogram_metrics` when a real
    `clickhouse-server` process is configured with
    `filesystem_cache_expose_prometheus_eviction_metrics=true` on a
    disk-backed cache and a workload
    drives evictions through it.
    """
    node.query(
        """
        CREATE TABLE eviction_metrics_test (id UInt64, blob String CODEC(NONE))
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'cache_metrics_policy', min_bytes_for_wide_part = 0
        """
    )

    # Cache is 100 KiB; each INSERT writes ~800 KiB so the cache must evict.
    # Re-reading then forces further evictions as old segments get displaced.
    for batch in range(3):
        node.query(
            "INSERT INTO eviction_metrics_test SELECT number + ?, repeat('x', 8192) FROM numbers(100)"
            .replace("?", str(batch * 100))
        )
        node.query("SELECT sum(length(blob)) FROM eviction_metrics_test SETTINGS enable_filesystem_cache = 1")

    evictions = sum_dim("filesystem_cache_evictions_total")
    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical"
    )

    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total") > 0
    hits = sum_hist("filesystem_cache_evicted_segment_hits")
    sizes = sum_hist("filesystem_cache_evicted_segment_size_bytes")
    assert int(evictions) == hits, f"counter={evictions} hits_observations={hits}"
    assert int(evictions) == sizes, f"counter={evictions} size_observations={sizes}"
    assert sum_dim("filesystem_cache_evictions_by_client_total") > 0

    # Verify the `queue` label is correctly populated for SLRU. Initial inserts
    # land in probationary; subsequent reads promote segments to protected. At
    # least some evictions must be labelled `probationary` or `protected` — if
    # all were `unknown` the queue_type lookup inside EvictionCandidates::evict
    # silently regressed (e.g. dynamic-resize path losing original_queue_types).
    slru_queue_evictions = float(node.query(
        "SELECT coalesce(sum(value), 0) FROM system.dimensional_metrics "
        "WHERE metric = 'filesystem_cache_evictions_total' "
        "AND labels['queue'] IN ('probationary', 'protected')"
    ).strip())
    assert slru_queue_evictions > 0, (
        f"No evictions with probationary/protected queue label — all unknown?\n{debug}"
    )

    # SLRU is configured; repeated SELECTs over the same blob data promote
    # probationary segments to protected, which must show up in the
    # promotions counter (same flag enables it).
    assert sum_dim("filesystem_cache_slru_promotions_total") > 0, (
        "SLRU promotion counter did not advance:\n" + debug
    )
    # `filesystem_cache_expose_prometheus_eviction_metrics_per_client` is
    # also enabled in the test config.
    assert sum_dim("filesystem_cache_slru_promotions_by_client_total") > 0, (
        "Per-client SLRU promotion counter did not advance:\n" + debug
    )
