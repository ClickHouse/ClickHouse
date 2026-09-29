import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    user_configs=["users.d/cache_on_write.xml"],
    stay_alive=True,
)

CACHE_NAME = "cache_with_eviction_metrics"

DIMENSIONAL_METRICS = [
    "filesystem_cache_evictions_total",
    "filesystem_cache_evicted_bytes_total",
    "filesystem_cache_evictions_by_user_total",
    "filesystem_cache_evicted_bytes_by_user_total",
]

HISTOGRAM_METRICS = [
    "filesystem_cache_evicted_segment_hits",
    "filesystem_cache_evicted_segment_size_bytes",
    "filesystem_cache_evicted_segment_hits_by_user",
    "filesystem_cache_evicted_segment_size_bytes_by_user",
]

PER_USER_METRICS = [
    "filesystem_cache_evictions_by_user_total",
    "filesystem_cache_evicted_bytes_by_user_total",
    "filesystem_cache_evicted_segment_hits_by_user",
    "filesystem_cache_evicted_segment_size_bytes_by_user",
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def sum_dim(metric, where="1"):
    return float(node.query(
        f"SELECT toFloat64(coalesce(sum(value), 0)) "
        f"FROM system.dimensional_metrics "
        f"WHERE metric = '{metric}' AND {where}"
    ).strip())


def sum_hist(metric, where="1"):
    """`system.histogram_metrics` exposes Prometheus-cumulative bucket rows
    keyed by `le`; the `+Inf` row carries the total observation count for
    each label-set. Sum across label-sets to get the family total."""
    return int(node.query(
        f"SELECT toUInt64(coalesce(sum(value), 0)) "
        f"FROM system.histogram_metrics "
        f"WHERE metric = '{metric}' AND labels['le'] = '+Inf' AND {where}"
    ).strip())


def cache_label_filter():
    return f"labels['cache_name'] = '{CACHE_NAME}'"


def user_label_filter():
    return (
        f"{cache_label_filter()} "
        "AND mapContains(labels, 'user_id') "
        "AND labels['user_id'] != ''"
    )


def assert_metric_labels(table, metric):
    bad_rows = node.query(
        f"""
        SELECT metric, labels
        FROM system.{table}
        WHERE metric = '{metric}'
          AND value > 0
          AND (
              NOT mapContains(labels, 'cache_name')
              OR labels['cache_name'] != '{CACHE_NAME}'
          )
        FORMAT TSV
        """
    )
    assert bad_rows == "", f"{metric} rows without expected cache label:\n{bad_rows}"

    if metric in PER_USER_METRICS:
        bad_user_rows = node.query(
            f"""
            SELECT metric, labels
            FROM system.{table}
            WHERE metric = '{metric}'
              AND value > 0
              AND (NOT mapContains(labels, 'user_id') OR labels['user_id'] = '')
            FORMAT TSV
            """
        )
        assert bad_user_rows == "", f"{metric} rows without user_id label:\n{bad_user_rows}"


def test_filesystem_cache_eviction_metrics(start_cluster):
    """
    Verify that `filesystem_cache_*` eviction metrics are populated when
    `expose_prometheus_eviction_metrics` is set in the disk config.
    """
    node.query(f"SYSTEM DROP FILESYSTEM CACHE '{CACHE_NAME}'")
    node.query("DROP TABLE IF EXISTS eviction_metrics_test")
    node.query(
        """
        CREATE TABLE eviction_metrics_test (id UInt64, blob String CODEC(NONE))
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'cache_metrics_policy', min_bytes_for_wide_part = 0
        """
    )

    # Snapshot all eight families before writes.
    evictions_before        = sum_dim("filesystem_cache_evictions_total", cache_label_filter())
    bytes_before            = sum_dim("filesystem_cache_evicted_bytes_total", cache_label_filter())
    hits_before             = sum_hist("filesystem_cache_evicted_segment_hits", cache_label_filter())
    size_before             = sum_hist("filesystem_cache_evicted_segment_size_bytes", cache_label_filter())
    by_user_before          = sum_dim("filesystem_cache_evictions_by_user_total", user_label_filter())
    bytes_by_user_before    = sum_dim("filesystem_cache_evicted_bytes_by_user_total", user_label_filter())
    hits_by_user_before     = sum_hist("filesystem_cache_evicted_segment_hits_by_user", user_label_filter())
    size_by_user_before     = sum_hist("filesystem_cache_evicted_segment_size_bytes_by_user", user_label_filter())

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

    for metric in DIMENSIONAL_METRICS:
        assert_metric_labels("dimensional_metrics", metric)
    for metric in HISTOGRAM_METRICS:
        assert_metric_labels("histogram_metrics", metric)

    evictions = sum_dim("filesystem_cache_evictions_total", cache_label_filter()) - evictions_before
    assert evictions > 0, f"Aggregate eviction counter did not advance:\n{debug}"
    assert sum_dim("filesystem_cache_evicted_bytes_total", cache_label_filter()) - bytes_before > 0

    # Each eviction fires exactly one `observe` on each histogram family, so the
    # `+Inf` bucket delta must equal the eviction counter delta.
    hits_delta = sum_hist("filesystem_cache_evicted_segment_hits", cache_label_filter()) - hits_before
    size_delta = sum_hist("filesystem_cache_evicted_segment_size_bytes", cache_label_filter()) - size_before
    assert hits_delta == int(evictions), (
        f"hits histogram delta {hits_delta} != eviction counter {int(evictions)}:\n{debug}"
    )
    assert size_delta == int(evictions), (
        f"size histogram delta {size_delta} != eviction counter {int(evictions)}:\n{debug}"
    )

    # Per-user variants (`expose_prometheus_eviction_metrics_per_user=1` in config).
    assert sum_dim("filesystem_cache_evictions_by_user_total", user_label_filter()) - by_user_before > 0
    assert sum_dim("filesystem_cache_evicted_bytes_by_user_total", user_label_filter()) - bytes_by_user_before > 0
    assert sum_hist("filesystem_cache_evicted_segment_hits_by_user", user_label_filter()) - hits_by_user_before > 0, (
        f"per-user hits histogram did not advance:\n{debug}"
    )
    assert sum_hist("filesystem_cache_evicted_segment_size_bytes_by_user", user_label_filter()) - size_by_user_before > 0, (
        f"per-user size histogram did not advance:\n{debug}"
    )
