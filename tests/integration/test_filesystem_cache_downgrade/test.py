import pytest

from helpers.cluster import ClickHouseCluster

# A released version whose cache loader does not know the current cache file name.
# It must skip such files instead of loading them and then failing to find them on read.
OLD_VERSION = "26.6.4.55"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # System logs are disabled so that only the filesystem cache decides whether the older
        # server starts: its log tables can be unloadable for reasons unrelated to this test.
        cluster.add_instance(
            "node",
            image="clickhouse/clickhouse-server",
            tag=OLD_VERSION,
            main_configs=[
                "configs/storage_conf.xml",
                "configs/zz_disable_system_logs.xml",
            ],
            with_installed_binary=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_downgrade_with_populated_cache(started_cluster):
    node = started_cluster.instances["node"]

    node.restart_with_latest_version()

    node.query("DROP TABLE IF EXISTS test SYNC")
    node.query(
        """
        CREATE TABLE test (a Int32)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS storage_policy = 'cached'
        """
    )
    node.query("INSERT INTO test SELECT number FROM numbers(100)")
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    assert node.query("SELECT sum(a) FROM test") == "4950\n"

    # Restart so that the files read while loading the table (`format_version.txt` among them)
    # also end up in the cache. Failing to read such a file breaks startup of the older server.
    node.restart_clickhouse()
    assert node.query("SELECT sum(a) FROM test") == "4950\n"
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    # The downgrade: the cache directory written by the newer server is reused as is.
    node.restart_with_original_version()

    assert node.query("SELECT sum(a) FROM test") == "4950\n"
