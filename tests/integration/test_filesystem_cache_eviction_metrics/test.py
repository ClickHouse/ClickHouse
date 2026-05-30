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


def _drive_evictions(extra_settings=None):
    """
    Fill the cache via write-through INSERTs, then trigger evictions by
    reading data that was pushed out of the cache by later batches.

    With `cache_on_write_operations=1` on the disk, each INSERT batch
    fills the 100 KiB cache. After 3 batches (each 800 KiB of blob data),
    the cache holds only the most recent data (batch-2). Reading batch-0
    is then a cache miss that requires evicting batch-2 segments — those
    evictions fire the callback with the caller-supplied settings active.
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
    # Three batches of 100 rows × 8 KiB = 2.4 MiB total.  Each INSERT fills
    # the 100 KiB cache, evicting the previous batch's entries.
    for batch in range(3):
        node.query(
            "INSERT INTO eviction_metrics_test "
            "SELECT number + {offset}, repeat('x', 8192) FROM numbers(100)".format(
                offset=batch * 100
            )
        )

    base = {"enable_filesystem_cache": "1"}
    if extra_settings:
        base.update(extra_settings)

    # batch-0 was evicted by the later INSERTs; reading it forces cache misses
    # that must evict the current cache occupants (batch-2 data).
    node.query(
        "SELECT sum(length(blob)) FROM eviction_metrics_test WHERE id < 100",
        settings=base,
    )


def _assert_eviction_metrics(evictions_before, by_client_before, per_client):
    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric LIKE 'filesystem_cache_%' FORMAT Vertical"
    )
    evictions = sum_dim("filesystem_cache_evictions_total") - evictions_before
    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total") > 0

    slru_queue_evictions = float(node.query(
        "SELECT coalesce(sum(value), 0) FROM system.dimensional_metrics "
        "WHERE metric = 'filesystem_cache_evictions_total' "
        "AND labels['queue'] IN ('probationary', 'protected')"
    ).strip())
    assert slru_queue_evictions > 0, (
        f"No evictions with probationary/protected queue label — all unknown?\n{debug}"
    )

    by_client = sum_dim("filesystem_cache_evictions_by_client_total") - by_client_before
    if per_client:
        assert by_client > 0, f"Per-client counter did not advance:\n{debug}"
    else:
        assert by_client == 0, f"Per-client counter advanced but setting was off:\n{debug}"


def test_eviction_metrics_per_query_settings(start_cluster):
    """
    Metrics are enabled via the SQL SETTINGS clause on each query —
    the per-query configuration path.
    """
    evictions_before = sum_dim("filesystem_cache_evictions_total")
    by_client_before = sum_dim("filesystem_cache_evictions_by_client_total")

    _drive_evictions({
        "filesystem_cache_expose_prometheus_eviction_metrics": "1",
        "filesystem_cache_expose_prometheus_eviction_metrics_per_client": "1",
    })

    _assert_eviction_metrics(evictions_before, by_client_before, per_client=True)


def test_eviction_metrics_global_settings(start_cluster):
    """
    Metrics are enabled by passing settings at the connection level (simulating
    a user-profile or SET-level global configuration), without embedding them
    in the SQL SETTINGS clause.
    """
    evictions_before = sum_dim("filesystem_cache_evictions_total")
    by_client_before = sum_dim("filesystem_cache_evictions_by_client_total")

    # Settings are passed via the query helper (HTTP/TCP settings param),
    # not via a SQL SETTINGS clause — this simulates global/profile configuration.
    _drive_evictions({
        "filesystem_cache_expose_prometheus_eviction_metrics": "1",
    })

    _assert_eviction_metrics(evictions_before, by_client_before, per_client=False)
