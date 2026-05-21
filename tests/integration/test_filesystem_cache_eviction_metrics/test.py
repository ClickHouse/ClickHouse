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
    disk-backed cache and a workload drives evictions through it.

    Eviction model
    --------------
    The SLRU cache is 100 KiB (50 KiB probationary + 50 KiB protected).

    1. INSERT batches 0–2 (rows 0–299, ~800 KiB each) go to disk only;
       `cache_on_write_operations` is intentionally unset at the disk level
       so writes bypass the cache.
    2. Read batch-0 rows (id < 100) once → fills the PROBATIONARY queue.
       Because all rows are in one MergeTree part, MergeTree reads the blob
       column file sequentially; the first 50 KiB (5 × 10 KiB segments) land
       in probationary, the rest bypass the full cache.
    3. Read batch-0 rows again → the cached segments are cache-hits;
       `tryIncreasePriority` promotes them from probationary → PROTECTED.
       This is what we assert with `slru_promotions_total`.
    4. Read batch-1+2 rows (id >= 100) → completely different files (different
       MergeTree parts), so all segments are cache-misses.  The protected
       queue now holds the batch-0 segments which are fully RELEASED (query 3
       has finished), so `tryReserve` can evict them to make room.  That is
       what we assert with `evictions_total`.
    """
    node.query(
        """
        CREATE TABLE eviction_metrics_test (id UInt64, blob String CODEC(NONE))
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'cache_metrics_policy', min_bytes_for_wide_part = 0
        """
    )

    for batch in range(3):
        node.query(
            "INSERT INTO eviction_metrics_test "
            "SELECT number + {offset}, repeat('x', 8192) FROM numbers(100)".format(
                offset=batch * 100
            )
        )

    read_settings = "SETTINGS enable_filesystem_cache = 1"

    # Step 2: fill probationary with batch-0 segments.
    node.query(
        f"SELECT sum(length(blob)) FROM eviction_metrics_test WHERE id < 100 {read_settings}"
    )
    # Step 3: re-read batch-0 → cache-hits → promote to protected.
    node.query(
        f"SELECT sum(length(blob)) FROM eviction_metrics_test WHERE id < 100 {read_settings}"
    )
    # Step 4: read batch-1+2 (different parts, different cache keys) →
    # the released protected segments are evicted to make room.
    node.query(
        f"SELECT sum(length(blob)) FROM eviction_metrics_test WHERE id >= 100 {read_settings}"
    )

    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical"
    )

    evictions = sum_dim("filesystem_cache_evictions_total")
    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total") > 0
    hits = sum_hist("filesystem_cache_evicted_segment_hits")
    sizes = sum_hist("filesystem_cache_evicted_segment_size_bytes")
    assert int(evictions) == hits, f"counter={evictions} hits_observations={hits}"
    assert int(evictions) == sizes, f"counter={evictions} size_observations={sizes}"
    assert sum_dim("filesystem_cache_evictions_by_client_total") > 0

    # Verify the `queue` label is correctly populated for SLRU.  At least some
    # evictions must be labelled `probationary` or `protected` — if all were
    # `unknown` the queue_type lookup inside `EvictionCandidates::evict`
    # silently regressed.
    slru_queue_evictions = float(node.query(
        "SELECT coalesce(sum(value), 0) FROM system.dimensional_metrics "
        "WHERE metric = 'filesystem_cache_evictions_total' "
        "AND labels['queue'] IN ('probationary', 'protected')"
    ).strip())
    assert slru_queue_evictions > 0, (
        f"No evictions with probationary/protected queue label — all unknown?\n{debug}"
    )

    # SLRU is configured; step 3 promotes probationary segments to protected,
    # which must show up in the promotions counter.
    assert sum_dim("filesystem_cache_slru_promotions_total") > 0, (
        "SLRU promotion counter did not advance:\n" + debug
    )
    # `filesystem_cache_expose_prometheus_eviction_metrics_per_client` is
    # also enabled in the test config.
    assert sum_dim("filesystem_cache_slru_promotions_by_client_total") > 0, (
        "Per-client SLRU promotion counter did not advance:\n" + debug
    )
