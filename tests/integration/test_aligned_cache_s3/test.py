import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/s3_cache.xml"],
    stay_alive=True,
    with_minio=True,
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_cache_size(started_cluster):
    # Covers the completed-segment projection of the `use_real_disk_size` contract: after the
    # query below every touched file segment is fully downloaded, so the aligned reservation
    # footprint coincides with the block-aligned downloaded size, both live and after a restart.
    # The partial (`reserved_size > downloaded_size`) case is covered by
    # `test_cache_size_partial_segment_reserve_ahead`. Background download is disabled so that
    # nothing fills the cache between reading the metric and computing the expectation.
    table_name = "test_aligned_cache_size_s3"

    def expected_cache_size():
        block_size = int(
            node.exec_in_container(
                ["bash", "-c", "stat -f -c %S /tmp/s3_aligned_cache"],
                privileged=True,
                user="root",
            ).strip()
        )
        return int(
            node.query(
                f"""
                    SELECT sum(intDiv(downloaded_size + {block_size} - 1, {block_size}) * {block_size})
                    FROM system.filesystem_cache
                    WHERE cache_name = 'aligned_cache'
                """
            ).strip()
        )

    # drop full cache to count cache size later correctly
    node.query(
        """SYSTEM DROP FILESYSTEM CACHE;""",
    )

    cache_size = node.query(
        """
            SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize';
        """
    )

    assert int(cache_size) == 0

    node.query(
        f"""
            DROP TABLE IF EXISTS {table_name};
        """,
    )

    node.query(
        f"""
            CREATE TABLE {table_name}
            (
                `key` String,
                `value` String
            )
            ENGINE = MergeTree
            PRIMARY KEY key
            SETTINGS storage_policy='external';
        """,
    )

    node.query(
        f"""
            INSERT INTO {table_name} VALUES ('key1', 'value1');
        """,
    )

    node.query(
        f"""
            SELECT * FROM {table_name};
        """,
        settings={"filesystem_cache_allow_background_download": 0},
    )

    cache_size = node.query(
        """
            SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize';
        """
    )
    assert int(cache_size) == expected_cache_size()

    node.restart_clickhouse()

    cache_size = node.query(
        """
            SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize';
        """
    )

    assert int(cache_size) == expected_cache_size()


def test_aligned_cache_sub_block_write(started_cluster):
    # An encrypted disk on top of the aligned cache writes its small header file during
    # `checkAccess` at startup and then writes more bytes into the same filesystem block.
    # With `use_real_disk_size` the disk-accounted delta of such a sub-block write is zero,
    # which used to abort the cache reservation with `Logical error: 'size'`. Just having the
    # server start up with this disk already exercises the regression; the insert/read below
    # additionally drives sub-block writes through the cache.
    table_name = "test_aligned_cache_encrypted"

    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        f"""
            CREATE TABLE {table_name}
            (
                `key` String,
                `value` String
            )
            ENGINE = MergeTree
            PRIMARY KEY key
            SETTINGS storage_policy='external_encrypted';
        """,
    )

    node.query(f"INSERT INTO {table_name} VALUES ('key1', 'value1');")
    assert node.query(f"SELECT value FROM {table_name} WHERE key = 'key1';").strip() == "value1"

    # The server must still be alive (no logical error during the cache reservation).
    assert node.query("SELECT 1").strip() == "1"


def test_aligned_eviction_telemetry(started_cluster):
    # Regression for eviction telemetry under `use_real_disk_size`.
    #
    # `FilesystemCacheSize` is accounted in filesystem-block-aligned units, so the eviction
    # counters (`FilesystemCacheEvictedBytes` and the Prometheus totals) must report the same
    # unit. The aligned size has to be captured *before* the evicted segment is detached: a
    # detached segment loses its key metadata and `FileSegment::getDiskAccountedSize` then falls
    # back to the raw reserved size, which silently undercounts sub-block evictions.
    #
    # `aligned_cache_evict` has file segments smaller than one filesystem block and a `max_size`
    # far below the table footprint, so every evicted segment occupies exactly one block on disk.
    # The evicted-byte counter must therefore equal `evicted_segments * block_size`; the buggy raw
    # accounting reports strictly less.
    table_name = "test_aligned_eviction_telemetry"
    cache_path = "/tmp/s3_aligned_cache_evict"

    node.query("SYSTEM DROP FILESYSTEM CACHE;")

    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        f"""
            CREATE TABLE {table_name}
            (
                `key` UInt64,
                `value` String
            )
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS storage_policy='external_evict';
        """,
    )

    # Random strings do not compress, so the on-disk (and cached) footprint stays well above the
    # 32Ki cache size, which guarantees eviction regardless of the actual filesystem block size.
    node.query(
        f"INSERT INTO {table_name} SELECT number, randomString(64) FROM numbers(8192);"
    )

    # Read the whole table (all columns) to fill the cache and force eviction. Background download
    # is disabled so all downloads and evictions happen synchronously and are attributed to this
    # query in `system.query_log`.
    query_id = "aligned_eviction_telemetry_query"
    node.query(
        f"SELECT * FROM {table_name} FORMAT Null",
        query_id=query_id,
        settings={"filesystem_cache_allow_background_download": 0},
    )

    node.query("SYSTEM FLUSH LOGS;")

    evicted_bytes, evicted_segments = (
        node.query(
            f"""
                SELECT
                    ProfileEvents['FilesystemCacheEvictedBytes'],
                    ProfileEvents['FilesystemCacheEvictedFileSegments']
                FROM system.query_log
                WHERE query_id = '{query_id}' AND type = 'QueryFinish'
                ORDER BY event_time_microseconds DESC
                LIMIT 1
            """
        )
        .strip()
        .split("\t")
    )
    evicted_bytes = int(evicted_bytes)
    evicted_segments = int(evicted_segments)

    block_size = int(
        node.exec_in_container(
            ["bash", "-c", f"stat -f -c %S {cache_path}"],
            privileged=True,
            user="root",
        ).strip()
    )

    # `max_file_segment_size` (512) is not larger than any real filesystem block, so every evicted
    # segment is charged for exactly one block. Aligned accounting reports `segments * block_size`;
    # the buggy raw accounting reports the (smaller) sum of written bytes.
    assert block_size >= 512
    assert evicted_segments > 0
    assert evicted_bytes == evicted_segments * block_size


def test_cache_size_partial_segment_reserve_ahead(started_cluster):
    # Regression for the aligned *reservation* accounting, which `test_cache_size` cannot see.
    #
    # `use_real_disk_size` charges a file segment by the block-aligned `reserved_size`, not by the
    # downloaded size. With reserve-ahead (`reserve_granularity`) a read reserves a whole granule
    # past the download offset, so a partial read of a large file leaves the file segment in the
    # `reserved_size > downloaded_size` state. When the segment is completed, the surplus has to be
    # returned in the same unit it was charged in, that is
    # `alignFileSize(reserved_size) - alignFileSize(downloaded_size)`. Returning the raw
    # `reserved_size - downloaded_size` instead leaves `FilesystemCacheSize` (and `current_size` in
    # `system.filesystem_cache_settings`) permanently out of step with what the cache occupies on
    # disk.
    #
    # `aligned_cache_reserve_ahead` sets `reserve_granularity` to 4Mi and the table below is much
    # larger than that, while the query reads a single granule, so the reserve-ahead surplus is
    # guaranteed to be non-trivial.
    table_name = "test_aligned_cache_reserve_ahead"
    cache_name = "aligned_cache_reserve_ahead"
    cache_path = "/tmp/s3_aligned_cache_reserve_ahead"
    reserve_granularity = 4 * 1024 * 1024

    node.query("SYSTEM DROP FILESYSTEM CACHE;")
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
    node.query(
        f"""
            CREATE TABLE {table_name}
            (
                `key` UInt64,
                `value` String
            )
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS storage_policy='external_reserve_ahead';
        """,
    )

    # Random strings do not compress, so the column file is far larger than one reserve granule.
    node.query(
        f"INSERT INTO {table_name} SELECT number, randomString(64) FROM numbers(400000);"
    )

    bytes_on_disk = int(
        node.query(
            f"""
                SELECT sum(bytes_on_disk) FROM system.parts
                WHERE table = '{table_name}' AND active
            """
        ).strip()
    )
    assert bytes_on_disk > 4 * reserve_granularity

    # A single-granule read: it downloads a small prefix of a multi-megabyte file segment while
    # reserve-ahead reserves a whole granule. Background download is disabled, otherwise the rest
    # of the segment would be filled in and the segment would no longer be partial.
    node.query(
        f"SELECT value FROM {table_name} WHERE key = 0 FORMAT Null",
        settings={"filesystem_cache_allow_background_download": 0},
    )

    block_size = int(
        node.exec_in_container(
            ["bash", "-c", f"stat -f -c %S {cache_path}"],
            privileged=True,
            user="root",
        ).strip()
    )

    downloaded_sizes = [
        int(line)
        for line in node.query(
            f"""
                SELECT downloaded_size FROM system.filesystem_cache
                WHERE cache_name = '{cache_name}'
            """
        ).split()
    ]
    assert downloaded_sizes

    expected_cache_size = sum(
        (size + block_size - 1) // block_size * block_size for size in downloaded_sizes
    )

    # The read really was partial: it downloaded far less than the reserved granule, so the cache
    # must hold much less than the table's on-disk footprint.
    assert max(downloaded_sizes) < reserve_granularity
    assert 0 < expected_cache_size < bytes_on_disk // 2

    cache_size = int(
        node.query(
            "SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize'"
        ).strip()
    )
    assert cache_size == expected_cache_size

    current_size = int(
        node.query(
            f"""
                SELECT current_size FROM system.filesystem_cache_settings
                WHERE cache_name = '{cache_name}'
            """
        ).strip()
    )
    assert current_size == expected_cache_size
