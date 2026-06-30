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


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def metric_value(metric):
    return int(node.query(
        f"SELECT toUInt64(coalesce(sum(value), 0)) "
        f"FROM system.dimensional_metrics "
        f"WHERE metric = '{metric}' AND labels['cache_name'] = '{CACHE_NAME}'"
    ).strip())


def cache_setting(setting):
    return int(node.query(
        f"SELECT {setting} "
        f"FROM system.filesystem_cache_settings "
        f"WHERE cache_name = '{CACHE_NAME}'"
    ).strip())


def test_filesystem_cache_usage_metrics(start_cluster):
    """
    Verify that `filesystem_cache_*` usage metrics are populated when
    `expose_prometheus_cache_usage_metrics_per_user` is set in the disk config.
    """
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

    debug = node.query(
        "SELECT * FROM system.dimensional_metrics "
        "WHERE metric IN ('filesystem_cache_size_bytes', 'filesystem_cache_elements') "
        "FORMAT Vertical"
    )

    current_size = cache_setting("current_size")
    current_elements = cache_setting("current_elements_num")

    assert current_size > 0
    assert current_elements > 0
    assert metric_value("filesystem_cache_size_bytes") == current_size, debug
    assert metric_value("filesystem_cache_elements") == current_elements, debug

    labelled_users = int(node.query(
        f"SELECT count() "
        f"FROM system.dimensional_metrics "
        f"WHERE metric = 'filesystem_cache_size_bytes' "
        f"AND labels['cache_name'] = '{CACHE_NAME}' "
        f"AND labels['user_id'] != '' "
        f"AND value > 0"
    ).strip())
    assert labelled_users > 0, debug
