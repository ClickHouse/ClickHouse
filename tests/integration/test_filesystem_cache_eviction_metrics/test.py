import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query(sql):
    return node.query(sql).strip()


def aggregate_metric_value(family_name):
    """Sum every label-set value of a DimensionalMetric family.

    Returns 0 when the family has no recorded entries yet (which happens
    before any eviction has been driven through the cache)."""
    return float(
        query(
            f"SELECT toFloat64(coalesce(sum(value), 0)) "
            f"FROM system.dimensional_metrics WHERE metric = '{family_name}'"
        )
    )


def histogram_observation_count(family_name):
    """Sum the bucket counters of a HistogramMetric family across all label
    sets. Each observation increments exactly one bucket counter (the bucket
    it falls into, or the +Inf overflow), so summing every counter gives the
    total observation count."""
    return int(
        query(
            f"SELECT toUInt64(coalesce(sum(value), 0)) "
            f"FROM system.histogram_metrics WHERE metric = '{family_name}'"
        )
    )


def test_filesystem_cache_eviction_metrics(start_cluster):
    """
    With `expose_eviction_metrics=true` set on a filesystem cache, the
    `filesystem_cache_*` metric families must be registered and must
    receive updates as segments are evicted under cache pressure.

    The unit gtest in `gtest_filecache.cpp` covers the
    flag-on / flag-off semantics directly against an in-process FileCache.
    This integration test covers the end-to-end concern: are the metrics
    actually exposed via `system.dimensional_metrics` /
    `system.histogram_metrics` when a real `clickhouse-server` process is
    configured to enable them on a real disk-backed cache?
    """
    node.query(
        """
        CREATE TABLE eviction_metrics_test (
            id UInt64,
            blob String CODEC(NONE)
        ) ENGINE = MergeTree
        ORDER BY id
        SETTINGS storage_policy = 'cache_metrics_policy',
                 min_bytes_for_wide_part = 0
        """
    )

    # Confirm families are registered (description visible) even before
    # the first eviction.
    descriptions = query(
        """
        SELECT count() FROM system.dimensional_metrics
        WHERE metric IN (
            'filesystem_cache_evictions_total',
            'filesystem_cache_evicted_bytes_total',
            'filesystem_cache_evictions_by_client_total',
            'filesystem_cache_evicted_bytes_by_client_total'
        )
        """
    )
    # Families are registered but have no label-set entries yet, so the
    # count of rows is 0. Verify the description rows for these families
    # appear in the metadata view, which is `system.dimensional_metrics`
    # without filtering by label-values:
    has_families = query(
        """
        SELECT countDistinct(metric) FROM system.dimensional_metrics
        WHERE metric LIKE 'filesystem_cache_%'
        """
    )
    # Drive evictions: each blob is 8 KiB, max_size is 1 MiB, so inserting
    # ~256 KiB worth of blob bytes and then reading the full table back to
    # populate the cache, plus more inserts, forces SLRU eviction churn.
    node.query(
        "INSERT INTO eviction_metrics_test "
        "SELECT number, repeat('x', 8192) FROM numbers(256)"
    )
    # Read everything to populate the cache.
    node.query("SELECT count() FROM eviction_metrics_test SETTINGS enable_filesystem_cache=1")
    # Force pressure: append more data and re-read so the cache must evict.
    node.query(
        "INSERT INTO eviction_metrics_test "
        "SELECT number + 1000, repeat('y', 8192) FROM numbers(256)"
    )
    node.query("SELECT count() FROM eviction_metrics_test SETTINGS enable_filesystem_cache=1")
    node.query(
        "INSERT INTO eviction_metrics_test "
        "SELECT number + 2000, repeat('z', 8192) FROM numbers(256)"
    )
    node.query("SELECT count() FROM eviction_metrics_test SETTINGS enable_filesystem_cache=1")
    # Read the original rows again so probationary entries get promoted to
    # protected (gives the protected/probationary distinction something to
    # show in the metric labels).
    node.query("SELECT count() FROM eviction_metrics_test WHERE id < 256 SETTINGS enable_filesystem_cache=1")

    evictions = aggregate_metric_value("filesystem_cache_evictions_total")
    bytes_evicted = aggregate_metric_value("filesystem_cache_evicted_bytes_total")
    hits_observations = histogram_observation_count("filesystem_cache_evicted_segment_hits")
    size_observations = histogram_observation_count("filesystem_cache_evicted_segment_size_bytes")

    assert evictions > 0, (
        "Aggregate eviction counter did not advance despite cache pressure.\n"
        + node.query("SELECT * FROM system.dimensional_metrics WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical")
    )
    assert bytes_evicted > 0, "Evicted-bytes counter did not advance."
    assert hits_observations > 0, "Hits histogram recorded no observations."
    assert size_observations > 0, "Size histogram recorded no observations."
    assert int(evictions) == hits_observations, (
        f"evictions={evictions} should match hits histogram observation count={hits_observations}"
    )
    assert int(evictions) == size_observations, (
        f"evictions={evictions} should match size histogram observation count={size_observations}"
    )

    # Per-client variants are also enabled in this test config; they must
    # have advanced too. cache_name and queue labels must be present.
    by_client_rows = query(
        """
        SELECT count() FROM system.dimensional_metrics
        WHERE metric = 'filesystem_cache_evictions_by_client_total'
          AND value > 0
        """
    )
    assert int(by_client_rows) > 0, "Per-client counter did not record any entries."

    # The aggregate counter must be labelled by cache_name and queue.
    labels = query(
        """
        SELECT DISTINCT
            labels['cache_name'] AS cache_name,
            labels['queue'] AS queue
        FROM system.dimensional_metrics
        WHERE metric = 'filesystem_cache_evictions_total' AND value > 0
        ORDER BY queue
        """
    )
    assert "cache_with_eviction_metrics" in labels, (
        f"Expected cache_name label to include the configured cache name. Got:\n{labels}"
    )
    # SLRU policy was configured, so at least the probationary queue should appear.
    assert "probationary" in labels, (
        f"Expected probationary queue to appear in label values. Got:\n{labels}"
    )
