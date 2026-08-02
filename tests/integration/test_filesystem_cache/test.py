import logging
import os
import random
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock
from helpers.utility import SafeThread, generate_values, replace_config

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "config.d/storage_conf.xml",
                "config.d/filesystem_caches_path.xml",
            ],
            user_configs=[
                "users.d/cache_on_write_operations.xml",
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
            "cache_dynamic_resize",
            main_configs=[
                "config.d/cache_dynamic_resize.xml",
            ],
        )
        cluster.add_instance(
            "node_force_read_through_cache_on_merge",
            main_configs=[
                "config.d/storage_conf.xml",
                "config.d/force_read_through_cache_for_merges.xml",
                "config.d/filesystem_caches_path.xml",
            ],
            user_configs=[
                "users.d/cache_on_write_operations.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def non_shared_cluster():
    """
    For tests that cannot run in parallel against the same node/cluster (see test_custom_cached_disk, which relies on
    changing server settings at runtime)
    """
    try:
        # Randomize the cluster name
        cluster = ClickHouseCluster(f"{__file__}_non_shared_{random.randint(0, 10**7)}")
        cluster.add_instance(
            "node_no_filesystem_caches_path",
            main_configs=[
                "config.d/storage_conf.xml",
                "config.d/remove_filesystem_caches_path.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting test-exclusive cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def wait_for_cache_initialized(node, cache_path, max_attempts=50):
    initialized = False
    attempts = 0
    while not initialized:
        query_result = node.query(
            "SELECT path FROM system.filesystem_cache_settings WHERE is_initialized"
        )
        initialized = cache_path in query_result

        if initialized:
            break

        time.sleep(0.1)
        attempts += 1
        if attempts >= max_attempts:
            raise "Stopped waiting for cache to be initialized"


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
            name = 'parallel_loading_test',
            path = 'parallel_loading_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000,
            load_metadata_threads = 30);
        """
    )

    wait_for_cache_initialized(node, "parallel_loading_test")

    node.query(
        """
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
    keys = (
        node.query(
            "SELECT distinct(key) FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
        )
        .strip()
        .splitlines()
    )

    node.restart_clickhouse()
    wait_for_cache_initialized(node, "parallel_loading_test")

    # < because of additional files loaded into cache on server startup.
    assert cache_count <= int(node.query("SELECT count() FROM system.filesystem_cache"))
    keys_set = ",".join(["'" + x + "'" for x in keys])
    assert cache_state == node.query(
        f"SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE key in ({keys_set}) ORDER BY key, file_segment_range_begin, size"
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

    node.query("SYSTEM DROP FILESYSTEM CACHE;")
    for table in ["test", "test2"]:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table} SYNC;

            CREATE TABLE {table} (key UInt32, value String)
            Engine=MergeTree()
            ORDER BY value
            SETTINGS disk = disk(
                type = cache,
                name = '{table}',
                path = '{cache_path}',
                disk = 'hdd_blob',
                max_file_segment_size = '1Ki',
                boundary_alignment = '1Ki',
                cache_on_write_operations=1,
                max_size = '1Mi');
            """
        )

        wait_for_cache_initialized(node, cache_path)

        node.query(
            f"""
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

    node.query("SYSTEM DROP FILESYSTEM CACHE;")
    for table in ["cache1", "cache2"]:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table} SYNC;

            CREATE TABLE {table} (key UInt32, value String)
            Engine=MergeTree()
            ORDER BY value
            SETTINGS disk = '{table}';
            """
        )

        wait_for_cache_initialized(node, "cache1")

        node.query(
            f"""
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


def test_custom_cached_disk(non_shared_cluster):
    node = non_shared_cluster.instances["node_no_filesystem_caches_path"]

    assert "Cannot create cached custom disk without" in node.query_and_get_error(
        f"""
        DROP TABLE IF EXISTS test SYNC;
        CREATE TABLE test (a Int32)
        ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS disk = disk(type = cache, path = 'kek', max_size = 10, disk = 'hdd_blob');
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
    SETTINGS disk = disk(type = cache, name = 'custom_cached', path = 'kek', max_size = 10, disk = 'hdd_blob');
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
            "rm /etc/clickhouse-server/config.d/remove_filesystem_caches_path.xml",
        ]
    )
    node.restart_clickhouse()

    node.query(
        f"""
    CREATE TABLE test2 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached2', path = 'kek2', max_size = 10, disk = 'hdd_blob');
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
    SETTINGS disk = disk(type = cache, name = 'custom_cached3', path = 'kek3', max_size = 10, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/custom_caches/kek3"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached3'"
        ).strip()
    )

    assert "Filesystem cache absolute path must lie inside" in node.query_and_get_error(
        f"""
    CREATE TABLE test4 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached4', path = '/kek4', max_size = 10, disk = 'hdd_blob');
    """
    )

    node.query(
        f"""
    CREATE TABLE test4 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached4', path = '/var/lib/clickhouse/custom_caches/kek4', max_size = 10, disk = 'hdd_blob');
    """
    )

    assert (
        "/var/lib/clickhouse/custom_caches/kek4"
        == node.query(
            "SELECT cache_path FROM system.disks WHERE name = 'custom_cached4'"
        ).strip()
    )


def test_force_filesystem_cache_on_merges(cluster):
    def test(node, forced_read_through_cache_on_merge):
        def to_int(value):
            if value == "":
                return 0
            else:
                return int(value)

        r_cache_count = to_int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'CachedReadBufferCacheWriteBytes'"
            )
        )

        w_cache_count = to_int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'CachedWriteBufferCacheWriteBytes'"
            )
        )

        node.query(
            """
            DROP TABLE IF EXISTS test SYNC;

            CREATE TABLE test (key UInt32, value String)
            Engine=MergeTree()
            ORDER BY value
            SETTINGS disk = disk(
                type = cache,
                name = 'force_cache_on_merges',
                path = 'force_cache_on_merges',
                disk = 'hdd_blob',
                max_file_segment_size = '1Ki',
                cache_on_write_operations = 1,
                boundary_alignment = '1Ki',
                max_size = '10Gi',
                max_elements = 10000000,
                load_metadata_threads = 30);
            """
        )

        wait_for_cache_initialized(node, "force_cache_on_merges")

        node.query(
            """
            SYSTEM DROP FILESYSTEM CACHE;
            INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000000;
            INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000000;
            """
        )
        assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0
        assert int(node.query("SELECT max(size) FROM system.filesystem_cache")) == 1024

        w_cache_count_2 = int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'CachedWriteBufferCacheWriteBytes'"
            )
        )
        assert w_cache_count_2 > w_cache_count

        r_cache_count_2 = to_int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'CachedReadBufferCacheWriteBytes'"
            )
        )
        assert r_cache_count_2 == r_cache_count

        assert node.query(
            "select current_size from system.filesystem_cache_settings where cache_name = 'force_cache_on_merges'"
        ) == node.query("select sum(downloaded_size) from system.filesystem_cache")

        node.query("SYSTEM DROP FILESYSTEM CACHE")
        node.query("OPTIMIZE TABLE test FINAL")

        r_cache_count_3 = to_int(
            node.query(
                "SELECT value FROM system.events WHERE name = 'CachedReadBufferCacheWriteBytes'"
            )
        )

        if forced_read_through_cache_on_merge:
            assert r_cache_count_3 > r_cache_count
        else:
            assert r_cache_count_3 == r_cache_count

    node = cluster.instances["node_force_read_through_cache_on_merge"]
    test(node, True)
    node = cluster.instances["node"]
    test(node, False)


def test_system_sync_filesystem_cache(cluster):
    node = cluster.instances["node"]
    node.query(
        f"""
DROP TABLE IF EXISTS test;
SYSTEM DROP FILESYSTEM CACHE;

CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            max_size = '10Gi',
            path = "test_system_sync_filesystem_cache_{uuid.uuid4()}",
            cache_policy = 'lru',
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    wait_for_cache_initialized(node, "test_system_sync_filesystem_cache")

    node.query(
        """
INSERT INTO test SELECT 1, 'test';
    """
    )

    query_id = f"system_sync_filesystem_cache_1_{uuid.uuid4()}"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )

    key, offset = (
        node.query(
            f"""
    SYSTEM FLUSH LOGS;
    SELECT key, offset FROM system.filesystem_cache_log WHERE query_id = '{query_id}' ORDER BY size DESC LIMIT 1;
    """
        )
        .strip()
        .split("\t")
    )

    cache_path = node.query(
        f"SELECT cache_path FROM system.filesystem_cache WHERE key = '{key}' and file_segment_range_begin = {offset}"
    )

    node.exec_in_container(["bash", "-c", f"rm {cache_path}"])

    assert key in node.query("SYSTEM SYNC FILESYSTEM CACHE")

    node.query("SELECT * FROM test FORMAT Null")
    assert key not in node.query("SYSTEM SYNC FILESYSTEM CACHE")

    query_id = f"system_sync_filesystem_cache_2_{uuid.uuid4()}"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )

    key, offset = (
        node.query(
            f"""
    SYSTEM FLUSH LOGS;
    SELECT key, offset FROM system.filesystem_cache_log WHERE query_id = '{query_id}' ORDER BY size DESC LIMIT 1;
    """
        )
        .strip()
        .split("\t")
    )
    cache_path = node.query(
        f"SELECT cache_path FROM system.filesystem_cache WHERE key = '{key}' and file_segment_range_begin = {offset}"
    )
    assert len(cache_path) > 0

    node.exec_in_container(["bash", "-c", f"echo -n 'fff' > {cache_path}"])

    assert key in node.query("SYSTEM SYNC FILESYSTEM CACHE")

    node.query("SELECT * FROM test FORMAT Null")

    assert key not in node.query("SYSTEM SYNC FILESYSTEM CACHE")


def test_keep_up_size_ratio(cluster):
    node = cluster.instances["node"]
    max_elements = 20
    elements_ratio = 0.5
    cache_name = "keep_up_size_ratio"
    node.query(
        f"""
DROP TABLE IF EXISTS test;

CREATE TABLE test (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            name = {cache_name},
            max_size = '100Ki',
            max_elements = {max_elements},
            max_file_segment_size = 10,
            boundary_alignment = 10,
            path = "test_keep_up_size_ratio",
            keep_free_space_size_ratio = 0.5,
            keep_free_space_elements_ratio = {elements_ratio},
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    wait_for_cache_initialized(node, "test_keep_up_size_ratio")

    node.query(
        """
INSERT INTO test SELECT randomString(200);
    """
    )

    query_id = "test_keep_up_size_ratio_1"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )
    count = int(
        node.query(
            f"""
    SYSTEM FLUSH LOGS;
    SELECT uniqExact(concat(key, toString(offset)))
    FROM system.filesystem_cache_log
    WHERE read_type = 'READ_FROM_FS_AND_DOWNLOADED_TO_CACHE';
    """
        )
    )
    assert count > max_elements

    expected = 10
    for _ in range(100):
        elements = int(
            node.query(
                f"SELECT count() FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )
        if elements <= expected:
            break
        time.sleep(1)
    assert elements <= expected


cache_dynamic_resize_config = """
<clickhouse>
    <storage_configuration>
        <disks>
            <hdd_blob>
                <type>local_blob_storage</type>
                <path>/</path>
            </hdd_blob>
            <cache_dynamic_resize>
                <type>cache</type>
                <disk>hdd_blob</disk>
                <max_size>{}</max_size>
                <max_elements>{}</max_elements>
                <max_file_segment_size>10</max_file_segment_size>
                <boundary_alignment>10</boundary_alignment>
                <path>./cache_dynamic_reload/</path>
            </cache_dynamic_resize>
            <cache_dynamic_resize_disabled>
                <type>cache</type>
                <disk>hdd_blob</disk>
                <max_size>{}</max_size>
                <max_elements>{}</max_elements>
                <max_file_segment_size>10</max_file_segment_size>
                <boundary_alignment>10</boundary_alignment>
                <allow_dynamic_cache_resize>0</allow_dynamic_cache_resize>
                <path>./cache_dynamic_reload_disabled/</path>
            </cache_dynamic_resize_disabled>
        </disks>
    </storage_configuration>
    <filesystem_cache_log>
            <database>system</database>
            <table>filesystem_cache_log</table>
    </filesystem_cache_log>
</clickhouse>
"""


def test_dynamic_resize(cluster):
    node = cluster.instances["cache_dynamic_resize"]
    max_elements = 20
    cache_name = "cache_dynamic_resize"
    node.query(
        f"""
DROP TABLE IF EXISTS test;
SYSTEM DROP FILESYSTEM CACHE;
CREATE TABLE test (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = '{cache_name}', min_bytes_for_wide_part = 10485760;
    """
    )

    node.query(
        """
INSERT INTO test SELECT randomString(200);
SELECT * FROM test;
    """
    )

    def get_downloaded_size():
        return int(
            node.query(
                f"SELECT sum(downloaded_size) FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )

    def get_queue_size():
        return int(
            node.query(
                f"SELECT current_size FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
            )
        )

    def get_downloaded_elements():
        return int(
            node.query(
                f"SELECT count() FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )

    def get_queue_elements():
        return int(
            node.query(
                f"SELECT current_elements_num FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
            )
        )

    size = get_downloaded_size()
    assert size > 100
    assert size == get_queue_size()

    elements = get_downloaded_elements()
    assert elements > 10
    assert elements == get_queue_elements()

    default_config = cache_dynamic_resize_config.format(100000, 100, 100000, 100)
    new_config = cache_dynamic_resize_config.format(100000, 10, 100000, 100)
    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize.xml", new_config
    )

    node.query("SYSTEM RELOAD CONFIG")

    assert 10 == get_downloaded_elements()
    assert 10 == get_queue_elements()

    node.query(f"SYSTEM ENABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict")

    new_config = cache_dynamic_resize_config.format(100000, 5, 100000, 100)
    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize.xml", new_config
    )

    node.query("SYSTEM RELOAD CONFIG")

    assert 10 == get_queue_elements()
    assert 10 == get_downloaded_elements()

    assert 100000 == int(
        node.query(
            f"SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )
    assert 10 == int(
        node.query(
            f"SELECT max_elements FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )

    node.query(f"SYSTEM DISABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict")
    node.query("SYSTEM RELOAD CONFIG")

    assert 5 == get_downloaded_elements()
    assert 5 == get_queue_elements()

    assert 100000 == int(
        node.query(
            f"SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )
    assert 5 == int(
        node.query(
            f"SELECT max_elements FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )

    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize.xml", default_config
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert 100000 == int(
        node.query(
            f"SELECT max_size FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )
    assert 100 == int(
        node.query(
            f"SELECT max_elements FROM system.filesystem_cache_settings WHERE cache_name = '{cache_name}'"
        )
    )


def test_filesystem_cache_log(cluster):
    node = cluster.instances["node"]
    node.query(
        """
DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            max_size = '10Gi',
            path = "test_filesystem_cache_log",
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    node.query(
        """
INSERT INTO test SELECT 1, 'test';
    """
    )

    query_id = "system_filesystem_cache_log"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 0",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")
    assert 0 == int(
        node.query(
            f"SELECT count() FROM system.filesystem_cache_log WHERE query_id = '{query_id}'"
        )
    )

    query_id = "system_filesystem_cache_log_2"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")
    assert 0 < int(
        node.query(
            f"SELECT count() FROM system.filesystem_cache_log WHERE query_id = '{query_id}'"
        )
    )


def test_dynamic_resize_disabled(cluster):
    node = cluster.instances["cache_dynamic_resize"]
    max_elements = 20
    cache_name = "cache_dynamic_resize_disabled"
    node.query(
        f"""
DROP TABLE IF EXISTS test;
SYSTEM DROP FILESYSTEM CACHE;
CREATE TABLE test (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = '{cache_name}', min_bytes_for_wide_part = 10485760;
    """
    )

    node.query(
        """
INSERT INTO test SELECT randomString(200);
SELECT * FROM test;
    """
    )

    def get_downloaded_size():
        return int(
            node.query(
                f"SELECT sum(downloaded_size) FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )

    def get_downloaded_elements():
        return int(
            node.query(
                f"SELECT count() FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )

    size = get_downloaded_size()
    assert size > 100

    elements = get_downloaded_elements()
    assert elements > 10

    default_config = cache_dynamic_resize_config.format(100000, 100, 100000, 100)
    new_config = cache_dynamic_resize_config.format(100000, 100, 100000, 10)
    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize.xml", new_config
    )

    node.query("SYSTEM RELOAD CONFIG")

    assert size == get_downloaded_size()
    assert elements == get_downloaded_elements()

    assert node.contains_in_log(
        f"FileCache({cache_name}): Filesystem cache size was modified, but dynamic cache resize is disabled"
    )
    # Return config back to initial state.
    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize.xml", default_config
    )


def test_max_size_ratio(cluster):
    node = cluster.instances["node"]
    node.query(
        """
        DROP TABLE IF EXISTS test SYNC;
        CREATE TABLE test (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = 'cache_with_max_size_ratio'
        """
    )
    assert node.contains_in_log("Using max_size as ratio 0.7 to total disk space on path /var/log/clickhouse/fs-cache/max_size_ratio")


def test_finished_download_time(cluster):
    node = cluster.instances["node"]
    name = f"test_finished_download_time_{uuid.uuid4()}"
    node.query(
        f"""
        DROP TABLE IF EXISTS test SYNC;
        CREATE TABLE test (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            name = '{name}',
            path = '{name}/',
            max_size = '100Mi',
            disk = 'hdd_blob');
        """
    )
    node.query("INSERT INTO test SELECT number, toString(number) FROM numbers(100)")
    node.query("SELECT * FROM test FORMAT Null")
    time.sleep(2)
    elapsed_time = node.query(
        f"SELECT now() - finished_download_time FROM system.filesystem_cache WHERE cache_name = '{name}' and state = 'DOWNLOADED' ORDER BY finished_download_time DESC LIMIT 1"
    )
    assert len(elapsed_time) > 0
    assert int(elapsed_time) > 1
    assert int(elapsed_time) < 5
