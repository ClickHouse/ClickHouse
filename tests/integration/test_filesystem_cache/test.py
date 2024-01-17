import logging
import time
import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock, start_mock_servers
from helpers.utility import generate_values, replace_config, SafeThread

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "config.d/storage_conf.xml",
            ],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_caches_with_same_path",
            main_configs=[
                "config.d/storage_conf_2.xml",
            ],
        )
        cluster.add_instance(
            "node_no_filesystem_caches_path",
            main_configs=[
                "config.d/storage_conf.xml",
                "config.d/remove_filesystem_caches_path.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("node_name", ["node"])
def test_parallel_cache_loading_on_startup(cluster, node_name):
    node = cluster.instances[node_name]
    node.query(
        """
        DROP TABLE IF EXISTS test SYNC;

        CREATE TABLE test (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            path = 'paralel_loading_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000,
            load_metadata_threads = 30);

        SYSTEM DROP FILESYSTEM CACHE;
        INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000000;
        SELECT * FROM test FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache")) == 1024
    count = int(node.query("SELECT count() FROM test"))

    cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE size > 0")
    )
    cache_state = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
    )

    node.restart_clickhouse()

    assert cache_count == int(node.query("SELECT count() FROM system.filesystem_cache"))
    assert cache_state == node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache ORDER BY key, file_segment_range_begin, size"
    )

    assert node.contains_in_log("Loading filesystem cache with 30 threads")
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache")) == 1024
    assert (
        int(
            node.query(
                "SELECT value FROM system.events WHERE event = 'FilesystemCacheLoadMetadataMicroseconds'"
            )
        )
        > 0
    )
    node.query("SELECT * FROM test FORMAT Null")
    assert count == int(node.query("SELECT count() FROM test"))


@pytest.mark.parametrize("node_name", ["node"])
def test_caches_with_the_same_configuration(cluster, node_name):
    node = cluster.instances[node_name]
    cache_path = "cache1"

    node.query(f"SYSTEM DROP FILESYSTEM CACHE;")
    for table in ["test", "test2"]:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table} SYNC;

            CREATE TABLE {table} (key UInt32, value String)
            Engine=MergeTree()
            ORDER BY value
            SETTINGS disk = disk(
                type = cache,
                name = {table},
                path = '{cache_path}',
                disk = 'hdd_blob',
                max_file_segment_size = '1Ki',
                boundary_alignment = '1Ki',
                cache_on_write_operations=1,
                max_size = '1Mi');

            SET enable_filesystem_cache_on_write_operations=1;
            INSERT INTO {table} SELECT * FROM generateRandom('a Int32, b String')
            LIMIT 1000;
            """
        )

    size = int(
        node.query(
            "SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize'"
        )
    )
    assert (
        node.query(
            "SELECT cache_name, sum(size) FROM system.filesystem_cache GROUP BY cache_name ORDER BY cache_name"
        ).strip()
        == f"test\t{size}\ntest2\t{size}"
    )

    table = "test3"
    assert (
        "Found more than one cache configuration with the same path, but with different cache settings"
        in node.query_and_get_error(
            f"""
        DROP TABLE IF EXISTS {table} SYNC;

        CREATE TABLE {table} (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            name = {table},
            path = '{cache_path}',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            cache_on_write_operations=0,
            max_size = '2Mi');
        """
        )
    )


@pytest.mark.parametrize("node_name", ["node_caches_with_same_path"])
def test_caches_with_the_same_configuration_2(cluster, node_name):
    node = cluster.instances[node_name]
    cache_path = "cache1"

    node.query(f"SYSTEM DROP FILESYSTEM CACHE;")
    for table in ["cache1", "cache2"]:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table} SYNC;

            CREATE TABLE {table} (key UInt32, value String)
            Engine=MergeTree()
            ORDER BY value
            SETTINGS disk = '{table}';

            SET enable_filesystem_cache_on_write_operations=1;
            INSERT INTO {table} SELECT * FROM generateRandom('a Int32, b String')
            LIMIT 1000;
            """
        )

    size = int(
        node.query(
            "SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize'"
        )
    )
    assert (
        node.query(
            "SELECT cache_name, sum(size) FROM system.filesystem_cache GROUP BY cache_name ORDER BY cache_name"
        ).strip()
        == f"cache1\t{size}\ncache2\t{size}"
    )


def test_custom_cached_disk(cluster):
    node = cluster.instances["node_no_filesystem_caches_path"]

    assert "Cannot create cached custom disk without" in node.query_and_get_error(
        f"""
        DROP TABLE IF EXISTS test SYNC;
        CREATE TABLE test (a Int32)
        ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS disk = disk(type = cache, path = 'kek', max_size = 1, disk = 'hdd_blob');
        """
    )

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/filesystem_caches_path.xml
        """,
        ]
    )
    node.restart_clickhouse()

    node.query(
        f"""
    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached', path = 'kek', max_size = 1, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/filesystem_caches/kek"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached'"
        ).strip()
    )

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
        <clickhouse>
            <custom_cached_disks_base_directory>/var/lib/clickhouse/custom_caches/</custom_cached_disks_base_directory>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/custom_filesystem_caches_path.xml
        """,
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm /etc/clickhouse-server/config.d/remove_filesystem_caches_path.xml"
        ]
    )
    node.restart_clickhouse()

    node.query(
        f"""
    CREATE TABLE test2 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached2', path = 'kek2', max_size = 1, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/custom_caches/kek2"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached2'"
        ).strip()
    )

    node.exec_in_container(
        ["bash", "-c", "rm /etc/clickhouse-server/config.d/filesystem_caches_path.xml"]
    )
    node.restart_clickhouse()

    node.query(
        f"""
    CREATE TABLE test3 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached3', path = 'kek3', max_size = 1, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/custom_caches/kek3"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached3'"
        ).strip()
    )

    assert "Filesystem cache path must lie inside" in node.query_and_get_error(
        f"""
    CREATE TABLE test4 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached4', path = '/kek4', max_size = 1, disk = 'hdd_blob');
    """
    )

    node.query(
        f"""
    CREATE TABLE test4 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached4', path = '/var/lib/clickhouse/custom_caches/kek4', max_size = 1, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/custom_caches/kek4"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached4'"
        ).strip()
    )
