import os

import pytest

from helpers.cluster import ClickHouseCluster

# Tests that the size of the cache of preprocessed polygons for the pointInPolygon
# function can be changed at runtime via the `point_in_polygon_cache_size` server
# setting. Lowering the limit evicts entries immediately, and setting it to 0
# empties the cache and disables it.
#
# Also tests that `SYSTEM DROP POINT IN POLYGON CACHE` evicts all entries while
# leaving the configured size limit unchanged, so the cache keeps caching.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/default.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")

POINT_IN_POLYGON_QUERY = """
SELECT sum(pointInPolygon((number % 10, intDiv(number, 10) % 10), [(0.5, 0.5), (8.5, 0.5), (8.5, 8.5), (0.5, 8.5)]))
FROM numbers(1000)
"""

CACHE_CELLS_QUERY = (
    "SELECT value FROM system.metrics WHERE metric = 'PointInPolygonCacheCells'"
)

CACHE_SIZE_LIMIT_QUERY = (
    "SELECT value FROM system.metrics WHERE metric = 'PointInPolygonCacheSizeLimit'"
)

SERVER_SETTING_QUERY = """
SELECT value, changeable_without_restart
FROM system.server_settings
WHERE name = 'point_in_polygon_cache_size'
"""


def test_point_in_polygon_cache_size_is_runtime_configurable(start_cluster):
    assert node.query(SERVER_SETTING_QUERY) == "268435456\tYes\n"

    # The first query preprocesses the constant polygon and caches it.
    assert node.query(POINT_IN_POLYGON_QUERY) == "640\n"
    assert node.query(CACHE_CELLS_QUERY) == "1\n"

    # Switch to a config with a cache size of 0, which disables the cache.
    # The new limit is applied immediately: the cache becomes empty right
    # after the reload, without running any query.
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disabled_cache.xml"),
        "/etc/clickhouse-server/config.d/default.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query(SERVER_SETTING_QUERY) == "0\tYes\n"
    assert node.query(CACHE_CELLS_QUERY) == "0\n"

    # Queries still work while the cache is disabled; the preprocessed
    # polygon is just not retained.
    assert node.query(POINT_IN_POLYGON_QUERY) == "640\n"
    assert node.query(CACHE_CELLS_QUERY) == "0\n"

    # Restore the original config: the cache accepts entries again.
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "default.xml"),
        "/etc/clickhouse-server/config.d/default.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query(POINT_IN_POLYGON_QUERY) == "640\n"
    assert node.query(CACHE_CELLS_QUERY) == "1\n"


def test_system_drop_point_in_polygon_cache(start_cluster):
    # Start from the default config so the cache is enabled regardless of what
    # other tests in this module did before.
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "default.xml"),
        "/etc/clickhouse-server/config.d/default.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert node.query(SERVER_SETTING_QUERY) == "268435456\tYes\n"

    # Populate the cache with one preprocessed polygon.
    assert node.query(POINT_IN_POLYGON_QUERY) == "640\n"
    assert node.query(CACHE_CELLS_QUERY) == "1\n"

    # Dropping the cache evicts all entries but, unlike setting the size to 0,
    # leaves the configured size limit unchanged.
    node.query("SYSTEM DROP POINT IN POLYGON CACHE")
    assert node.query(CACHE_CELLS_QUERY) == "0\n"
    assert node.query(CACHE_SIZE_LIMIT_QUERY) == "268435456\n"
    assert node.query(SERVER_SETTING_QUERY) == "268435456\tYes\n"

    # The cache is still enabled, so the next query repopulates it.
    assert node.query(POINT_IN_POLYGON_QUERY) == "640\n"
    assert node.query(CACHE_CELLS_QUERY) == "1\n"

    # The `SYSTEM CLEAR` spelling is an accepted alias and behaves the same.
    node.query("SYSTEM CLEAR POINT IN POLYGON CACHE")
    assert node.query(CACHE_CELLS_QUERY) == "0\n"
