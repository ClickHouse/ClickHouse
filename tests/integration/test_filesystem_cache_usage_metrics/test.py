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


SHARED_ALIAS_A = "shared_alias_a"
SHARED_ALIAS_B = "shared_alias_b"


def metric_value_for_cache(metric, cache_name):
    return int(node.query(
        f"SELECT toUInt64(coalesce(sum(value), 0)) "
        f"FROM system.dimensional_metrics "
        f"WHERE metric = '{metric}' AND labels['cache_name'] = '{cache_name}'"
    ).strip())


def cache_setting_for(setting, cache_name):
    return int(node.query(
        f"SELECT {setting} "
        f"FROM system.filesystem_cache_settings "
        f"WHERE cache_name = '{cache_name}'"
    ).strip())


def test_shared_cache_alias_usage_metrics(start_cluster):
    """
    Two cache disks that share the same path and identical settings alias a single
    `FileCache` instance in `FileCacheFactory`. The usage gauges must be reported once,
    under the primary (first-created) cache name, and must never be double-counted
    across the alias names, even though `system.filesystem_cache_settings` reports the
    same `current_size` / `current_elements_num` for every alias.
    """
    node.query(f"SYSTEM DROP FILESYSTEM CACHE '{SHARED_ALIAS_A}'")
    for table, policy in [
        ("shared_alias_a_tbl", "shared_alias_a_policy"),
        ("shared_alias_b_tbl", "shared_alias_b_policy"),
    ]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"""
            CREATE TABLE {table} (id UInt64, blob String CODEC(NONE))
            ENGINE = MergeTree ORDER BY id
            SETTINGS storage_policy = '{policy}', min_bytes_for_wide_part = 0
            """
        )
        node.query(
            f"INSERT INTO {table} SELECT number, repeat('x', 8192) FROM numbers(50)"
        )

    # Both alias names report the same physical usage in system.filesystem_cache_settings.
    size_a = cache_setting_for("current_size", SHARED_ALIAS_A)
    size_b = cache_setting_for("current_size", SHARED_ALIAS_B)
    assert size_a > 0
    assert size_a == size_b, f"current_size mismatch across aliases: {size_a} != {size_b}"

    elements_a = cache_setting_for("current_elements_num", SHARED_ALIAS_A)
    elements_b = cache_setting_for("current_elements_num", SHARED_ALIAS_B)
    assert elements_a > 0
    assert elements_a == elements_b, (
        f"current_elements_num mismatch across aliases: {elements_a} != {elements_b}"
    )

    debug = node.query(
        "SELECT metric, labels['cache_name'] AS cache_name, labels['user_id'] AS user_id, value "
        "FROM system.dimensional_metrics "
        "WHERE metric IN ('filesystem_cache_size_bytes', 'filesystem_cache_elements') "
        "AND labels['cache_name'] IN ('shared_alias_a', 'shared_alias_b') "
        "ORDER BY metric, cache_name, user_id FORMAT Vertical"
    )

    for metric, physical in [
        ("filesystem_cache_size_bytes", size_a),
        ("filesystem_cache_elements", elements_a),
    ]:
        value_a = metric_value_for_cache(metric, SHARED_ALIAS_A)
        value_b = metric_value_for_cache(metric, SHARED_ALIAS_B)
        # Reported once, under the primary cache name only: the total across the alias
        # names equals the single physical usage (not doubled).
        assert value_a + value_b == physical, (
            f"{metric}: {value_a} + {value_b} != {physical} (double-counted?)\n{debug}"
        )
        # Exactly one alias name carries the usage series.
        assert (value_a > 0) != (value_b > 0), (
            f"{metric}: exactly one alias name must carry usage\n{debug}"
        )
