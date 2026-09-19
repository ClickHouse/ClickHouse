import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    user_configs=["users.d/cache_on_write.xml"],
    stay_alive=True,
)

CACHE_NAME = "cache_with_usage_metrics"
METRICS = {
    "filesystem_cache_size_bytes": "current_size",
    "filesystem_cache_elements": "current_elements_num",
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_filesystem_cache_usage_metrics(start_cluster):
    node.query(f"SYSTEM DROP FILESYSTEM CACHE '{CACHE_NAME}'")
    node.query("DROP TABLE IF EXISTS usage_metrics_test")
    node.query(
        """
        CREATE TABLE usage_metrics_test (id UInt64, blob String CODEC(NONE))
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'cache_usage_metrics_policy', min_bytes_for_wide_part = 0
        """
    )

    node.query(
        "INSERT INTO usage_metrics_test "
        "SELECT number, repeat('x', 8192) FROM numbers(100)"
    )
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    current_size, current_elements = map(
        int,
        node.query(
            f"""
            SELECT current_size, current_elements_num
            FROM system.filesystem_cache_settings
            WHERE cache_name = '{CACHE_NAME}'
            """
        ).split(),
    )
    settings = {
        "current_size": current_size,
        "current_elements_num": current_elements,
    }
    # The `user_id` label is the filesystem cache client, which for a plain server
    # is `Context::getFilesystemCacheUser` and falls back to the server UUID when it
    # is not set (see `getCommonUserID` in `FileCache.cpp`). Pin the exact value, so
    # that a change of the label's meaning shows up as a test failure rather than
    # silently passing an "is not empty" check.
    metrics = {
        metric: (int(value), int(expected_users), int(distinct_users))
        for metric, value, expected_users, distinct_users in (
            row.split("\t")
            for row in node.query(
                f"""
                SELECT
                    metric,
                    toUInt64(sum(value)),
                    countIf(labels['user_id'] = toString(serverUUID())),
                    uniqExact(labels['user_id'])
                FROM system.dimensional_metrics
                WHERE metric IN ({", ".join(repr(metric) for metric in METRICS)})
                  AND labels['cache_name'] = '{CACHE_NAME}'
                GROUP BY metric
                """
            ).strip().splitlines()
        )
    }
    assert metrics.keys() == METRICS.keys()
    for metric, setting in METRICS.items():
        value, expected_users, distinct_users = metrics[metric]
        assert value == int(settings[setting]) > 0
        assert distinct_users == 1
        assert expected_users == 1

    node.query(f"SYSTEM DROP FILESYSTEM CACHE '{CACHE_NAME}'")
    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
    assert node.query(
        f"""
        SELECT metric
        FROM system.dimensional_metrics
        WHERE metric IN ({", ".join(repr(metric) for metric in METRICS)})
          AND labels['cache_name'] = '{CACHE_NAME}'
        """
    ) == ""
