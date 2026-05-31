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
    Verify that filesystem_cache_* eviction metrics are populated when
    expose_prometheus_eviction_metrics is set in the disk config.
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

    # Snapshot all eight families before writes.
    evictions_before        = sum_dim("filesystem_cache_evictions_total")
    bytes_before            = sum_dim("filesystem_cache_evicted_bytes_total")
    hits_before             = sum_hist("filesystem_cache_evicted_segment_hits")
    size_before             = sum_hist("filesystem_cache_evicted_segment_size_bytes")
    by_client_before        = sum_dim("filesystem_cache_evictions_by_client_total")
    bytes_by_client_before  = sum_dim("filesystem_cache_evicted_bytes_by_client_total")
    hits_by_client_before   = sum_hist("filesystem_cache_evicted_segment_hits_by_client")
    size_by_client_before   = sum_hist("filesystem_cache_evicted_segment_size_bytes_by_client")

    # Three batches of 100 rows × 8 KiB = 2.4 MiB total.
    # With cache_on_write_operations=1, each INSERT writes through the 100 KiB
    # cache. After the first 100 KiB fills, subsequent writes evict earlier
    # segments — those evictions fire the callback in the write thread.
    for batch in range(3):
        node.query(
            "INSERT INTO eviction_metrics_test "
            "SELECT number + {offset}, repeat('x', 8192) FROM numbers(100)".format(
                offset=batch * 100
            )
        )

    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical"
    )

    evictions = sum_dim("filesystem_cache_evictions_total") - evictions_before
    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total") - bytes_before > 0

    # Each eviction fires exactly one observe() on each histogram family, so the
    # +Inf bucket delta must equal the eviction counter delta.
    hits_delta = sum_hist("filesystem_cache_evicted_segment_hits") - hits_before
    size_delta = sum_hist("filesystem_cache_evicted_segment_size_bytes") - size_before
    assert hits_delta == int(evictions), (
        f"hits histogram delta {hits_delta} != eviction counter {int(evictions)}:\n{debug}"
    )
    assert size_delta == int(evictions), (
        f"size histogram delta {size_delta} != eviction counter {int(evictions)}:\n{debug}"
    )

    # Per-client variants (expose_prometheus_eviction_metrics_per_client=1 in config).
    assert sum_dim("filesystem_cache_evictions_by_client_total") - by_client_before > 0
    assert sum_dim("filesystem_cache_evicted_bytes_by_client_total") - bytes_by_client_before > 0
    assert sum_hist("filesystem_cache_evicted_segment_hits_by_client") - hits_by_client_before > 0, (
        f"per-client hits histogram did not advance:\n{debug}"
    )
    assert sum_hist("filesystem_cache_evicted_segment_size_bytes_by_client") - size_by_client_before > 0, (
        f"per-client size histogram did not advance:\n{debug}"
    )
