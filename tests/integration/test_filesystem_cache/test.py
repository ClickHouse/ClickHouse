import concurrent.futures
import logging
import os
import random
import threading
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

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
            with_zookeeper=True,
            with_minio=True,
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
            "cache_dynamic_resize_slru",
            main_configs=[
                "config.d/cache_dynamic_resize_slru.xml",
            ],
            stay_alive=True,
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
            with_zookeeper=True,
            stay_alive=True,
        )
        # Dedicated node: the background eviction push-fail test enables a process-global
        # failpoint, so it must not share a node with other tests.
        cluster.add_instance(
            "keep_up_push_fail",
            main_configs=[
                "config.d/storage_conf.xml",
                "config.d/filesystem_caches_path.xml",
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
            with_zookeeper=True,
            with_minio=True,
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
        SYSTEM DROP FILESYSTEM CACHE;

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
        SYSTEM CLEAR FILESYSTEM CACHE;
        INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000;
        SELECT * FROM test FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache WHERE cache_name = 'parallel_loading_test'")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache WHERE cache_name = 'parallel_loading_test'")) == 1024
    count = int(node.query("SELECT count() FROM test"))

    cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE size > 0 AND cache_name = 'parallel_loading_test'")
    )
    cache_state = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 AND cache_name = 'parallel_loading_test' ORDER BY key, file_segment_range_begin, size"
    )
    keys = (
        node.query(
            "SELECT distinct(key) FROM system.filesystem_cache WHERE size > 0 AND cache_name = 'parallel_loading_test' ORDER BY key, file_segment_range_begin, size"
        )
        .strip()
        .splitlines()
    )

    node.restart_clickhouse()
    wait_for_cache_initialized(node, "parallel_loading_test")

    # < because of additional files loaded into cache on server startup.
    assert cache_count <= int(node.query("SELECT count() FROM system.filesystem_cache WHERE cache_name = 'parallel_loading_test'"))
    keys_set = ",".join(["'" + x + "'" for x in keys])
    assert cache_state == node.query(
        f"SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE key in ({keys_set}) AND cache_name = 'parallel_loading_test' ORDER BY key, file_segment_range_begin, size"
    )

    assert node.contains_in_log("15 listing thread(s) and 15 loading thread(s)")
    assert int(node.query("SELECT count() FROM system.filesystem_cache WHERE cache_name = 'parallel_loading_test'")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache WHERE cache_name = 'parallel_loading_test'")) == 1024
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
def test_cache_file_size_in_name(cluster, node_name):
    """
    A fully downloaded regular cache file is named `<offset>_<size>`, which lets startup
    metadata loading read the size from the file name instead of `stat`-ing every file.
    This test verifies the on-disk naming, and that legacy `<offset>` files (without the
    size suffix) are still loaded correctly by falling back to a `stat`.
    """
    node = cluster.instances[node_name]
    node.query(
        """
        DROP TABLE IF EXISTS test_size_in_name SYNC;

        CREATE TABLE test_size_in_name (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            name = 'size_in_name_test',
            path = 'size_in_name_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000);
        """
    )

    wait_for_cache_initialized(node, "size_in_name_test")

    node.query(
        """
        SYSTEM CLEAR FILESYSTEM CACHE;
        INSERT INTO test_size_in_name SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000;
        SELECT * FROM test_size_in_name FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    cache_path = node.query(
        "SELECT cache_path FROM system.disks WHERE name = 'size_in_name_test'"
    ).strip()

    def list_segment_files():
        out = node.exec_in_container(
            ["bash", "-c", f"find {cache_path} -type f -printf '%f %s\\n'"]
        )
        files = []
        for line in out.splitlines():
            name, _, size = line.partition(" ")
            if name == "status" or name.endswith("_temporary"):
                continue
            files.append((name, int(size)))
        return files

    files = list_segment_files()
    assert len(files) > 0
    # Every regular segment file encodes its size in the name as `<offset>_<size>`,
    # and that size matches the file's actual size on disk.
    for name, size in files:
        assert "_" in name, f"segment file {name} has no size suffix"
        name_size = int(name.split("_", 1)[1])
        assert name_size == size, f"{name}: size in name {name_size} != actual {size}"

    def cache_segments():
        return set(
            node.query(
                "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0"
            )
            .strip()
            .splitlines()
        )

    # Every segment cached before the restart must survive it (startup may add more, hence subset).
    cache_state = cache_segments()
    assert len(cache_state) > 0

    # Restart: metadata is loaded by reading sizes from the file names (no stat).
    node.restart_clickhouse()
    wait_for_cache_initialized(node, "size_in_name_test")
    assert cache_state <= cache_segments()

    # Backward compatibility: rename the files to the legacy `<offset>` form (no size suffix)
    # and make sure they are still loaded on the next startup (the size is obtained with a stat).
    # The server must be stopped while we rewrite the file names: a running server keeps the
    # in-memory `<offset>_<size>` name (`hasSizeInFileName`) and would not find the renamed file.
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"find {cache_path} -type f -name '*_*' ! -name '*_temporary' "
            "-exec bash -c 'mv \"$1\" \"$(dirname \"$1\")/$(basename \"$1\" | cut -d_ -f1)\"' _ {} ';'",
        ]
    )
    # No file should carry a size suffix anymore.
    assert all("_" not in name for name, _ in list_segment_files())

    node.start_clickhouse()
    wait_for_cache_initialized(node, "size_in_name_test")
    assert cache_state <= cache_segments()
    node.query("SELECT * FROM test_size_in_name FORMAT Null")
    node.query("DROP TABLE test_size_in_name SYNC")


@pytest.mark.parametrize("node_name", ["node"])
def test_cache_file_truncated_size_in_name(cluster, node_name):
    """
    A fully downloaded regular cache file is named `<offset>_<size>`, and startup metadata loading
    trusts that size without a `stat`. If such a file is truncated outside ClickHouse, the segment is
    restored as fully downloaded but the on-disk file is shorter than recorded. Reading it must not raise
    a `LOGICAL_ERROR`: the broken cache entry is discarded and the data is re-fetched from the source.

    This covers both a shorter-than-recorded file and a zero-length file.
    """
    node = cluster.instances[node_name]
    node.query(
        """
        DROP TABLE IF EXISTS test_truncated_size_in_name SYNC;

        CREATE TABLE test_truncated_size_in_name (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            name = 'truncated_size_in_name_test',
            path = 'truncated_size_in_name_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000);
        """
    )

    wait_for_cache_initialized(node, "truncated_size_in_name_test")

    node.query(
        """
        SYSTEM CLEAR FILESYSTEM CACHE;
        INSERT INTO test_truncated_size_in_name SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000;
        SELECT * FROM test_truncated_size_in_name FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    expected_count = int(node.query("SELECT count() FROM test_truncated_size_in_name"))
    expected_sum = node.query("SELECT sum(cityHash64(key, value)) FROM test_truncated_size_in_name").strip()

    cache_path = node.query(
        "SELECT cache_path FROM system.disks WHERE name = 'truncated_size_in_name_test'"
    ).strip()

    def list_suffixed_segment_files():
        out = node.exec_in_container(
            ["bash", "-c", f"find {cache_path} -type f -printf '%p %f %s\\n'"]
        )
        files = []
        for line in out.splitlines():
            full_path, name, size = line.rsplit(" ", 2)
            if name == "status" or name.endswith("_temporary") or "_" not in name:
                continue
            files.append((full_path, int(size)))
        return files

    files = list_suffixed_segment_files()
    # Need at least two segment files to exercise both the short and the zero-length cases.
    assert len(files) >= 2, files

    # The server must be stopped while we corrupt the files: a running server holds the segments open.
    node.stop_clickhouse()

    # Truncate one suffixed file to half its size (non-empty but shorter than recorded) and another
    # to zero bytes. Their names still encode the full `<size>`, so startup trusts the larger size.
    short_path, short_size = files[0]
    zero_path, _ = files[1]
    node.exec_in_container(["bash", "-c", f"truncate -s {max(short_size // 2, 1)} '{short_path}'"])
    node.exec_in_container(["bash", "-c", f"truncate -s 0 '{zero_path}'"])

    node.start_clickhouse()
    wait_for_cache_initialized(node, "truncated_size_in_name_test")

    # Reading must not raise a LOGICAL_ERROR. A truncated segment is discarded and the read is
    # transparently re-routed to the source, so the part loads and the query succeeds. Retry
    # defensively in case a concurrent state transition surfaces a retryable error first.
    last_error = None
    succeeded = False
    for _ in range(20):
        try:
            node.query("SELECT * FROM test_truncated_size_in_name FORMAT Null")
            succeeded = True
            break
        except Exception as e:
            last_error = str(e)
            assert "LOGICAL_ERROR" not in last_error, last_error
            assert "Logical error" not in last_error, last_error

    assert succeeded, f"query did not recover, last error: {last_error}"

    # The data is intact after re-fetching the discarded segments from the source.
    assert int(node.query("SELECT count() FROM test_truncated_size_in_name")) == expected_count
    assert (
        node.query("SELECT sum(cityHash64(key, value)) FROM test_truncated_size_in_name").strip()
        == expected_sum
    )
    node.query("DROP TABLE test_truncated_size_in_name SYNC")


@pytest.mark.parametrize("node_name", ["node"])
def test_cache_file_truncated_size_in_name_concurrent_readers(cluster, node_name):
    """
    Concurrent-reader variant of `test_cache_file_truncated_size_in_name`.

    When several readers race on the same externally truncated `<offset>_<size>` cache file, one reader
    can discard/detach the segment between another reader opening the short file and re-checking its
    state. The losing reader must still bypass the cache and re-fetch from the source rather than keep its
    truncated descriptor and surface a `LOGICAL_ERROR`. Fire many parallel scans of the truncated data so
    that at least some of them read the corrupted segment simultaneously, and assert none of them raises a
    `LOGICAL_ERROR` and the data stays intact.
    """
    node = cluster.instances[node_name]
    node.query(
        """
        DROP TABLE IF EXISTS test_truncated_size_concurrent SYNC;

        CREATE TABLE test_truncated_size_concurrent (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            name = 'truncated_size_concurrent_test',
            path = 'truncated_size_concurrent_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignment = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000);
        """
    )

    wait_for_cache_initialized(node, "truncated_size_concurrent_test")

    node.query(
        """
        SYSTEM CLEAR FILESYSTEM CACHE;
        INSERT INTO test_truncated_size_concurrent SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000;
        SELECT * FROM test_truncated_size_concurrent FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0

    expected_count = int(node.query("SELECT count() FROM test_truncated_size_concurrent"))
    expected_sum = node.query(
        "SELECT sum(cityHash64(key, value)) FROM test_truncated_size_concurrent"
    ).strip()

    cache_path = node.query(
        "SELECT cache_path FROM system.disks WHERE name = 'truncated_size_concurrent_test'"
    ).strip()

    def list_suffixed_segment_files():
        out = node.exec_in_container(
            ["bash", "-c", f"find {cache_path} -type f -printf '%p %f %s\\n'"]
        )
        files = []
        for line in out.splitlines():
            full_path, name, size = line.rsplit(" ", 2)
            if name == "status" or name.endswith("_temporary") or "_" not in name:
                continue
            files.append((full_path, int(size)))
        return files

    files = list_suffixed_segment_files()
    assert len(files) >= 1, files

    # The server must be stopped while we corrupt the file: a running server holds the segment open.
    node.stop_clickhouse()

    # Truncate every suffixed file to half its size. Their names still encode the full `<size>`, so
    # startup trusts the larger size and every scan that reads them must self-heal.
    for short_path, short_size in files:
        node.exec_in_container(
            ["bash", "-c", f"truncate -s {max(short_size // 2, 1)} '{short_path}'"]
        )

    node.start_clickhouse()
    wait_for_cache_initialized(node, "truncated_size_concurrent_test")

    # Launch the scans as simultaneously as possible (a barrier releases them together) so several
    # readers touch the same truncated segments before the first discard removes them.
    num_readers = 16
    barrier = threading.Barrier(num_readers)

    def scan():
        barrier.wait()
        # A truncated segment is discarded and the read re-routed to the source. Retry defensively in
        # case a concurrent state transition surfaces a retryable error first; a `LOGICAL_ERROR` must
        # never appear.
        last_error = None
        for _ in range(20):
            try:
                node.query("SELECT * FROM test_truncated_size_concurrent FORMAT Null")
                return None
            except Exception as e:
                last_error = str(e)
                assert "LOGICAL_ERROR" not in last_error, last_error
                assert "Logical error" not in last_error, last_error
        return last_error

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_readers) as executor:
        errors = [f.result() for f in [executor.submit(scan) for _ in range(num_readers)]]

    assert all(e is None for e in errors), errors

    # The data is intact after re-fetching the discarded segments from the source.
    assert (
        int(node.query("SELECT count() FROM test_truncated_size_concurrent")) == expected_count
    )
    assert (
        node.query(
            "SELECT sum(cityHash64(key, value)) FROM test_truncated_size_concurrent"
        ).strip()
        == expected_sum
    )
    node.query("DROP TABLE test_truncated_size_concurrent SYNC")


@pytest.mark.parametrize("node_name", ["node"])
def test_bypass_cache_does_not_overread_non_last_segment(cluster, node_name):
    """
    Regression test for an over-read on the `REMOTE_FS_READ_BYPASS_CACHE` path.

    A non-last file segment read in bypass mode relies on the buffer being
    right-bounded (the read size is clamped to the range only for a single held
    segment). For readers without right-bounded support (local object storage)
    the bypass buffer must be wrapped into `BoundedReadBuffer`, otherwise it
    reads past the segment and trips a logical error in
    `CachedOnDiskReadBufferFromFile`.

    The `cache_filesystem_failure` failpoint with `skip_cache_on_disk_failure`
    leaves segments in `PARTIALLY_DOWNLOADED_NO_CONTINUATION`, so concurrent
    readers read the front segment in bypass mode while holding the next ones.
    """
    node = cluster.instances[node_name]
    cache_name = f"bypass_overread_{uuid.uuid4().hex[:8]}"
    table_name = f"bypass_overread_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table_name} SYNC;
            CREATE TABLE {table_name} (key UInt32, value String)
            ENGINE = MergeTree() ORDER BY key
            SETTINGS disk = disk(
                type = cache,
                name = '{cache_name}',
                path = '{cache_name}/',
                max_size = '1Gi',
                max_file_segment_size = 32768,
                boundary_alignment = 32768,
                skip_cache_on_disk_failure = true,
                disk = 'hdd_blob'
            );
            INSERT INTO {table_name} SELECT number, randomString(100) FROM numbers(100000);
            SYSTEM DROP FILESYSTEM CACHE;
            """
        )

        test_start = node.query("SELECT now()").strip()

        # Force every download write to fail so segments stay in
        # PARTIALLY_DOWNLOADED_NO_CONTINUATION and reads fall back to bypass.
        node.query("SYSTEM ENABLE FAILPOINT cache_filesystem_failure")
        try:
            # Concurrent readers: while one query leaves the front segment in a
            # bypass state, others read it together with the following segments.
            node.exec_in_container(
                [
                    "/usr/bin/clickhouse",
                    "benchmark",
                    "--iterations",
                    "200",
                    "--concurrency",
                    "50",
                    "--query",
                    f"SELECT * FROM {table_name} FORMAT Null",
                ]
            )
        finally:
            node.query("SYSTEM DISABLE FAILPOINT cache_filesystem_failure")

        # If the over-read aborted the server, the queries above raise a
        # connection error (and the cluster teardown reports the crash).
        # Otherwise make sure no logical error was recorded.
        node.query("SELECT 1")
        errors = int(
            node.query(
                f"SELECT count() FROM system.errors WHERE name = 'LOGICAL_ERROR' AND last_error_time >= '{test_start}'"
            ).strip()
        )
        assert errors == 0, f"LOGICAL_ERROR occurred on {node.name}"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


@pytest.mark.parametrize("node_name", ["node"])
def test_caches_with_the_same_configuration(cluster, node_name):
    node = cluster.instances[node_name]
    cache_path = "cache1"

    node.query("SYSTEM CLEAR FILESYSTEM CACHE;")
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

    node.query("SYSTEM CLEAR FILESYSTEM CACHE;")
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
        """
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
            """echo "
        <clickhouse>
            <filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>
        </clickhouse>
        " > /etc/clickhouse-server/config.d/filesystem_caches_path.xml
        """,
        ]
    )
    node.restart_clickhouse()

    node.query(
        """
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
            """echo "
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
        """
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
        """
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
        """
    CREATE TABLE test4 (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(type = cache, name = 'custom_cached4', path = '/kek4', max_size = 10, disk = 'hdd_blob');
    """
    )

    node.query(
        """
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


@pytest.mark.skip(reason="In private we always use cache for merges")
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
            SYSTEM CLEAR FILESYSTEM CACHE;
            INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000;
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

        node.query("SYSTEM CLEAR FILESYSTEM CACHE")
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
SYSTEM CLEAR FILESYSTEM CACHE;

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

    query_id = f"test_keep_up_size_ratio_1_{uuid.uuid4()}"
    node.query(
        "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )
    count = int(
        node.query(
            """
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


def test_keep_up_size_ratio_parallel_eviction(cluster):
    # Regression test for the parallel background eviction path:
    # a small remove batch with several remover threads forces the single collector
    # to hand many batches to multiple removers and finalize them as they come back.
    node = cluster.instances["node"]
    max_elements = 200
    cache_name = "keep_up_size_ratio_parallel"
    node.query(
        f"""
DROP TABLE IF EXISTS test_parallel_eviction;

CREATE TABLE test_parallel_eviction (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            name = {cache_name},
            max_size = '100Ki',
            max_elements = {max_elements},
            max_file_segment_size = 10,
            boundary_alignment = 10,
            path = "test_keep_up_size_ratio_parallel",
            keep_free_space_size_ratio = 0.5,
            keep_free_space_elements_ratio = 0.5,
            keep_free_space_remove_batch = 2,
            keep_free_space_eviction_threads = 3,
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    wait_for_cache_initialized(node, "test_keep_up_size_ratio_parallel")

    node.query(
        "INSERT INTO test_parallel_eviction SELECT randomString(200) FROM numbers(50);"
    )

    # Fill the cache well above max_elements so background keeping has to evict
    # many small file segments in many batches.
    query_id = "test_keep_up_size_ratio_parallel_1"
    node.query(
        "SELECT * FROM test_parallel_eviction FORMAT Null SETTINGS enable_filesystem_cache_log = 1",
        query_id=query_id,
    )
    count = int(
        node.query(
            f"""
    SYSTEM FLUSH LOGS;
    SELECT uniqExact(concat(key, toString(offset)))
    FROM system.filesystem_cache_log
    WHERE read_type = 'READ_FROM_FS_AND_DOWNLOADED_TO_CACHE' AND query_id = '{query_id}';
    """
        )
    )
    assert count > max_elements

    # keep_free_space_*_ratio = 0.5 over max_elements = 200 -> converge to ~100.
    expected = max_elements // 2
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

    # The data must stay fully readable after parallel eviction + finalization:
    # the cached read must return exactly what a cache-bypassing read returns.
    assert int(node.query("SELECT count() FROM test_parallel_eviction")) == 50
    assert node.query(
        "SELECT sum(cityHash64(a)) FROM test_parallel_eviction"
    ) == node.query(
        "SELECT sum(cityHash64(a)) FROM test_parallel_eviction SETTINGS enable_filesystem_cache = 0"
    )


def test_keep_up_size_ratio_push_fail(cluster):
    node = cluster.instances["keep_up_push_fail"]
    max_elements = 100
    cache_name = "keep_up_size_ratio_push_fail"
    node.query(
        f"""
DROP TABLE IF EXISTS test_push_fail;

CREATE TABLE test_push_fail (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            name = {cache_name},
            max_size = '10Mi',
            max_elements = {max_elements},
            max_file_segment_size = 10,
            boundary_alignment = 10,
            path = "test_keep_up_size_ratio_push_fail",
            keep_free_space_size_ratio = 0.9,
            keep_free_space_elements_ratio = 0.9,
            keep_free_space_remove_batch = 2,
            keep_free_space_eviction_threads = 3,
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    wait_for_cache_initialized(node, "test_keep_up_size_ratio_push_fail")

    def elems():
        return int(
            node.query(
                f"SELECT count() FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
            )
        )

    # keep_free_space_elements_ratio = 0.9 over max_elements = 100 -> target 10.
    expected = max_elements // 10

    node.query("SYSTEM ENABLE FAILPOINT file_cache_background_eviction_push_fail")
    try:
        node.query(
            "INSERT INTO test_push_fail SELECT randomString(1000) FROM numbers(500);"
        )
        node.query("SELECT * FROM test_push_fail FORMAT Null")

        # With the failpoint on, every collected batch fails to reach the remover workers,
        # so background keeping bails out (CANNOT_EVICT) and cannot drop below the fill level.
        node.wait_for_log_line("Background eviction workers take too much time")
        blocked = elems()
        assert blocked > expected
        time.sleep(3)
        assert elems() == blocked
    finally:
        node.query("SYSTEM DISABLE FAILPOINT file_cache_background_eviction_push_fail")

    # After the dropped batches are rolled back, background keeping must converge all the
    # way to the configured target - not just make partial progress - which proves no
    # entries were left stuck in the `Evicting` state.
    for _ in range(60):
        converged = elems()
        if converged <= expected:
            break
        time.sleep(1)
    assert converged <= expected

    assert int(node.query("SELECT count() FROM test_push_fail")) == 500
    assert node.query(
        "SELECT sum(cityHash64(a)) FROM test_push_fail"
    ) == node.query(
        "SELECT sum(cityHash64(a)) FROM test_push_fail SETTINGS enable_filesystem_cache = 0"
    )


def test_proactive_invalidated_entries_cleanup(cluster):
    node = cluster.instances["node"]
    cache_name = "proactive_invalidated_cleanup"
    # keep_free_space_*_ratio are left at their defaults (disabled), so the only
    # thing that purges invalidated (lazily-removed) priority queue entries is the
    # dedicated background cleanup task. max_size/max_elements are large enough to
    # hold everything, so no eviction happens (eviction would purge them itself).
    node.query(
        f"""
DROP TABLE IF EXISTS test_proactive_cleanup;

CREATE TABLE test_proactive_cleanup (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache,
            name = {cache_name},
            max_size = '1Gi',
            max_elements = 100000,
            max_file_segment_size = 10,
            boundary_alignment = 10,
            path = "test_proactive_invalidated_cleanup",
            invalidated_entries_cleanup_threshold = 5,
            invalidated_entries_cleanup_interval_ms = 500,
            disk = hdd_blob),
        min_bytes_for_wide_part = 10485760;
    """
    )

    wait_for_cache_initialized(node, "test_proactive_invalidated_cleanup")

    node.query("INSERT INTO test_proactive_cleanup SELECT randomString(2000);")
    node.query("SELECT * FROM test_proactive_cleanup FORMAT Null")

    cached = int(
        node.query(
            f"SELECT count() FROM system.filesystem_cache WHERE cache_name = '{cache_name}'"
        )
    )
    # We need clearly more than the cleanup threshold of invalidated entries.
    assert cached > 5

    def removed_count():
        return int(
            node.query(
                "SELECT sum(value) FROM system.events "
                "WHERE event = 'FilesystemCacheBackgroundRemovedInvalidatedEntries'"
            )
        )

    before = removed_count()

    # Removing the segments invalidates their priority queue entries lazily
    # (without taking the priority write lock), leaving them in the queue.
    node.query(f"SYSTEM DROP FILESYSTEM CACHE '{cache_name}'")

    removed = 0
    for _ in range(120):
        removed = removed_count() - before
        if removed >= cached:
            break
        time.sleep(0.5)

    assert removed >= cached


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
    cache_name = "cache_dynamic_resize"
    node.query(
        f"""
DROP TABLE IF EXISTS test;
SYSTEM CLEAR FILESYSTEM CACHE;
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

    node.query("SYSTEM ENABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict")

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

    node.query("SYSTEM DISABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict")
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

    wait_for_cache_initialized(node, "test_filesystem_cache_log")


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
    cache_name = "cache_dynamic_resize_disabled"
    node.query(
        f"""
DROP TABLE IF EXISTS test;
SYSTEM CLEAR FILESYSTEM CACHE;
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


@pytest.mark.parametrize("cache_policy", ["lru", "slru"])
def test_concurrent_eviction(cluster, cache_policy):
    """Stress-test concurrent eviction in filesystem cache with multiple readers."""
    import threading

    node = cluster.instances["node"]
    cache_name = f"bench_small_{cache_policy}_{uuid.uuid4().hex[:8]}"
    table_name = f"bench_eviction_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            f"""
            DROP TABLE IF EXISTS {table_name} SYNC;
            CREATE TABLE {table_name} (key UInt32, value String)
            ENGINE = MergeTree() ORDER BY key
            SETTINGS disk = disk(
                type = cache,
                name = '{cache_name}',
                path = '{cache_name}/',
                max_size = '1Mi',
                max_file_segment_size = 32768,
                boundary_alignment = 32768,
                cache_policy = '{cache_policy}',
                disk = 'hdd_blob'
            );
            INSERT INTO {table_name} SELECT number, randomString(100) FROM numbers(100000);
            """
        )

        test_start = node.query("SELECT now()").strip()

        stop_event = threading.Event()

        def drop_cache_loop():
            while not stop_event.is_set():
                node.query(f"SYSTEM CLEAR FILESYSTEM CACHE '{cache_name}'")

        drop_thread = threading.Thread(target=drop_cache_loop, daemon=True)
        drop_thread.start()

        try:
            node.exec_in_container(
                [
                    "/usr/bin/clickhouse",
                    "benchmark",
                    "--iterations",
                    "200",
                    "--concurrency",
                    "100",
                    "--query",
                    f"SELECT count() FROM {table_name} WHERE key < (randConstant() % 100000) OR key > (randConstant() % 100000) FORMAT Null",
                ]
            )
        finally:
            stop_event.set()
            drop_thread.join()

        errors = int(
            node.query(
                f"SELECT count() FROM system.errors WHERE name = 'LOGICAL_ERROR' AND last_error_time >= '{test_start}'"
            ).strip()
        )
        assert errors == 0, f"LOGICAL_ERROR occurred on {node.name}"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


cache_dynamic_resize_slru_config = """
<clickhouse>
    <storage_configuration>
        <disks>
            <hdd_blob>
                <type>local_blob_storage</type>
                <path>/</path>
            </hdd_blob>
            <cache_dynamic_resize_slru>
                <type>cache</type>
                <disk>hdd_blob</disk>
                <max_size>{max_size}</max_size>
                <max_elements>{max_elements}</max_elements>
                <max_file_segment_size>10</max_file_segment_size>
                <boundary_alignment>10</boundary_alignment>
                <cache_policy>SLRU</cache_policy>
                <allow_dynamic_cache_resize>1</allow_dynamic_cache_resize>
                <path>./cache_dynamic_resize_slru/</path>
            </cache_dynamic_resize_slru>
        </disks>
    </storage_configuration>
</clickhouse>
"""


def slru_config(max_size=100, max_elements=10):
    return cache_dynamic_resize_slru_config.format(
        max_size=max_size, max_elements=max_elements
    )


def test_dynamic_resize_slru(cluster):
    """Test that SLRU filesystem cache properly evicts from both protected and
    probationary queues when max_size and max_elements are shrunk via config reload,
    and that growing limits back works correctly."""
    node = cluster.instances["cache_dynamic_resize_slru"]
    cache_name = "cache_dynamic_resize_slru"

    node.query(
        f"""
DROP TABLE IF EXISTS test_slru1 SYNC;
DROP TABLE IF EXISTS test_slru2 SYNC;
SYSTEM CLEAR FILESYSTEM CACHE;
CREATE TABLE test_slru1 (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = '{cache_name}', min_bytes_for_wide_part = 10485760,
         serialization_info_version = 'basic';
INSERT INTO test_slru1 SELECT randomString(20);
CREATE TABLE test_slru2 (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = '{cache_name}', min_bytes_for_wide_part = 10485760,
         serialization_info_version = 'basic';
INSERT INTO test_slru2 SELECT randomString(20);
SYSTEM CLEAR FILESYSTEM CACHE;
    """
    )

    def get_cache_settings():
        row = node.query(
            f"SELECT max_size, max_elements FROM system.filesystem_cache_settings "
            f"WHERE cache_name = '{cache_name}'"
        ).strip()
        parts = row.split("\t")
        return int(parts[0]), int(parts[1])

    def get_downloaded_count():
        return int(
            node.query(
                f"SELECT count() FROM system.filesystem_cache "
                f"WHERE state = 'DOWNLOADED' AND cache_name = '{cache_name}'"
            )
        )

    def get_downloaded_size():
        return int(
            node.query(
                f"SELECT sum(downloaded_size) FROM system.filesystem_cache "
                f"WHERE state = 'DOWNLOADED' AND cache_name = '{cache_name}'"
            )
        )

    try:
        # Verify initial settings
        max_size, max_elements = get_cache_settings()
        assert max_size == 100
        assert max_elements == 10

        assert get_downloaded_count() == 0

        test_start = node.query("SELECT now()").strip()

        # Read table 1 twice to promote its segments into the protected queue
        node.query("SELECT * FROM test_slru1 FORMAT Null")
        node.query("SELECT * FROM test_slru1 FORMAT Null")

        # Read table 2 once -- its segments stay in the probationary queue
        node.query("SELECT * FROM test_slru2 FORMAT Null")

        assert get_downloaded_count() > 0
        assert get_downloaded_size() > 0

        # --- Shrink max_size from 100 to 10 ---
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=10, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")

        s, e = get_cache_settings()
        assert s == 10
        assert e == 10
        # Total cached bytes must not exceed the new limit
        assert get_downloaded_size() <= 10

        # --- Grow max_size back to 100 ---
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=100, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")

        s, e = get_cache_settings()
        assert s == 100
        assert e == 10

        # Re-read to populate the cache again
        node.query("SELECT * FROM test_slru1 FORMAT Null")
        node.query("SELECT * FROM test_slru2 FORMAT Null")
        assert get_downloaded_count() > 0
        assert get_downloaded_size() > 0

        # --- Shrink max_elements from 10 to 2 ---
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=100, max_elements=2),
        )
        node.query("SYSTEM RELOAD CONFIG")

        s, e = get_cache_settings()
        assert s == 100
        assert e == 2
        assert get_downloaded_count() <= 2

        # --- Grow max_elements back to 10 ---
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=100, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")

        s, e = get_cache_settings()
        assert s == 100
        assert e == 10

        # Verify the cache still works after all the resizing
        node.query("SELECT * FROM test_slru1 FORMAT Null")
        assert get_downloaded_count() > 0

        # No LOGICAL_ERROR should have occurred during resize operations
        errors = int(
            node.query(
                f"SELECT count() FROM system.errors "
                f"WHERE name = 'LOGICAL_ERROR' AND last_error_time >= '{test_start}'"
            ).strip()
        )
        assert errors == 0, "LOGICAL_ERROR occurred during SLRU resize test"

    finally:
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=100, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")
        node.query("DROP TABLE IF EXISTS test_slru1 SYNC")
        node.query("DROP TABLE IF EXISTS test_slru2 SYNC")


def test_dynamic_resize_slru_failpoint_eviction(cluster):
    """Test that SLRU filesystem cache resize gracefully handles eviction failures
    via the file_cache_dynamic_resize_fail_to_evict failpoint. When eviction fails,
    entries should be restored to their original queues and the cache should remain
    in a consistent state."""
    node = cluster.instances["cache_dynamic_resize_slru"]
    cache_name = "cache_dynamic_resize_slru"

    # Restore to known-good initial state
    node.replace_config(
        "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
        slru_config(max_size=100, max_elements=10),
    )
    node.query("SYSTEM RELOAD CONFIG")

    node.query(
        f"""
DROP TABLE IF EXISTS test_slru_fp SYNC;
SYSTEM CLEAR FILESYSTEM CACHE;
CREATE TABLE test_slru_fp (a String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = '{cache_name}', min_bytes_for_wide_part = 10485760,
         serialization_info_version = 'basic';
INSERT INTO test_slru_fp SELECT randomString(20);
SYSTEM CLEAR FILESYSTEM CACHE;
    """
    )

    get_downloaded_count = lambda: int(
        node.query(
            f"SELECT count() FROM system.filesystem_cache "
            f"WHERE state = 'DOWNLOADED' AND cache_name = '{cache_name}'"
        )
    )

    get_downloaded_size = lambda: int(
        node.query(
            f"SELECT sum(downloaded_size) FROM system.filesystem_cache "
            f"WHERE state = 'DOWNLOADED' AND cache_name = '{cache_name}'"
        )
    )

    get_max_size = lambda: int(
        node.query(
            f"SELECT max_size FROM system.filesystem_cache_settings "
            f"WHERE cache_name = '{cache_name}'"
        ).strip()
    )

    try:
        test_start = node.query("SELECT now()").strip()

        # Read twice to promote segments into the protected queue
        node.query("SELECT * FROM test_slru_fp FORMAT Null")
        node.query("SELECT * FROM test_slru_fp FORMAT Null")

        initial_count = get_downloaded_count()
        initial_size = get_downloaded_size()
        assert initial_count > 0
        assert initial_size > 0

        # Enable failpoint so eviction will fail
        node.query(
            "SYSTEM ENABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict"
        )

        # Anchor log position so we only check lines written from now on
        log_anchor = node.count_log_lines()

        # Attempt to shrink -- eviction will fail, so limits should stay
        # at old values (or somewhere between old and desired)
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=10, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")

        # Wait for the background resize thread to attempt (and fail) the resize.
        # Confirm the failure path actually ran by checking for the log message
        # emitted when eviction candidates fail.
        node.wait_for_log_line(
            "Having .* failed candidates",
            timeout=60,
            look_behind_lines=f"+{log_anchor}",
        )

        # Disable failpoint before checking state
        node.query(
            "SYSTEM DISABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict"
        )

        # After failed resize, limits should have reverted to prev_limits (100)
        assert get_max_size() == 100, (
            f"max_size should have reverted to 100 after failed resize, got {get_max_size()}"
        )

        # Entries should have been restored -- count and size should be
        # the same as before the failed resize attempt
        assert get_downloaded_count() == initial_count, (
            f"Entry count changed after failed resize: {initial_count} -> {get_downloaded_count()}"
        )
        assert get_downloaded_size() == initial_size, (
            f"Total size changed after failed resize: {initial_size} -> {get_downloaded_size()}"
        )

        # Cache should still be usable -- reads should work
        node.query("SELECT * FROM test_slru_fp FORMAT Null")

        # Now do a real resize (without failpoint) to verify cache is not corrupted
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=10, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")

        # Poll for the real resize to complete
        for _ in range(30):
            if get_max_size() == 10:
                break
            time.sleep(1)

        assert get_max_size() == 10, (
            f"Dynamic resize to 10 did not complete, max_size is {get_max_size()}"
        )

        assert get_downloaded_size() <= 10, (
            f"Cache size {get_downloaded_size()} exceeds new limit 10 after real resize"
        )

        # No LOGICAL_ERROR should have occurred
        errors = int(
            node.query(
                f"SELECT count() FROM system.errors "
                f"WHERE name = 'LOGICAL_ERROR' AND last_error_time >= '{test_start}'"
            ).strip()
        )
        assert errors == 0, "LOGICAL_ERROR occurred during SLRU failpoint resize test"

    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT file_cache_dynamic_resize_fail_to_evict"
        )
        node.replace_config(
            "/etc/clickhouse-server/config.d/cache_dynamic_resize_slru.xml",
            slru_config(max_size=100, max_elements=10),
        )
        node.query("SYSTEM RELOAD CONFIG")
        node.query("DROP TABLE IF EXISTS test_slru_fp SYNC")


def test_reserve_granularity_reclaims_surplus_after_read(cluster):
    # Regression test for reserve-ahead accounting: a sub-granule read must not keep a
    # whole `reserve_granularity` charged against the cache after the read buffer is
    # destroyed. With `reserve_granularity == boundary_alignment` the completion-time
    # `shrinkFileSegmentToDownloadedSize` rounds the downloaded size back up to the whole
    # range, so the reserve-ahead surplus has to be reclaimed explicitly; otherwise the
    # cache stays charged for bytes that were never written.
    node = cluster.instances["node"]

    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("DROP TABLE IF EXISTS test_reserve_granularity SYNC")
    node.query(
        """
        CREATE TABLE test_reserve_granularity (key UInt64, value String)
        Engine=MergeTree()
        ORDER BY key
        SETTINGS disk = disk(
            type = cache,
            name = 'reserve_granularity_cache',
            path = 'reserve_granularity_cache',
            disk = 'hdd_blob',
            max_size = '1Gi',
            max_file_segment_size = '4Mi',
            boundary_alignment = '4Mi',
            reserve_granularity = '4Mi',
            background_download_threads = 0,
            cache_on_write_operations = 0),
        index_granularity = 256,
        min_bytes_for_wide_part = 0
        """
    )
    node.query("SYSTEM STOP MERGES test_reserve_granularity")

    # cache_on_write_operations = 0, so the INSERT itself does not populate the cache.
    # Incompressible values make the column span many 4Mi file segments.
    node.query(
        "INSERT INTO test_reserve_granularity SELECT number, randomString(2000) FROM numbers(50000)"
    )

    # A single point read: downloads only a small (sub-granule) part of one file segment.
    node.query(
        "SELECT value FROM test_reserve_granularity WHERE key = 0 SETTINGS max_read_buffer_size = 65536"
    )

    # The read buffer is destroyed and no background download is configured, so the touched
    # segment is completed and shrunk. `size` is the (boundary-aligned) segment range, while
    # `downloaded_size` is what was actually written; the segment must be partially downloaded
    # for this test to exercise the reserve-ahead surplus at all.
    range_size = int(
        node.query(
            "SELECT sum(size) FROM system.filesystem_cache WHERE cache_name = 'reserve_granularity_cache'"
        )
    )
    downloaded = int(
        node.query(
            "SELECT sum(downloaded_size) FROM system.filesystem_cache WHERE cache_name = 'reserve_granularity_cache'"
        )
    )
    assert downloaded > 0
    assert range_size > downloaded, "expected at least one partially downloaded segment"

    # FilesystemCacheSize tracks the space charged against the cache (sum of reserved sizes).
    # After reclaiming the reserve-ahead surplus it must equal the actually downloaded bytes,
    # not the rounded-up range. Without the fix it would equal `range_size`.
    reserved = int(
        node.query("SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize'")
    )
    assert reserved == downloaded, f"reserved {reserved} != downloaded {downloaded} (range {range_size})"

    node.query("DROP TABLE test_reserve_granularity SYNC")
