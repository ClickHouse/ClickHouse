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
    table_name = "test_aligned_cache_size_s3"
    cache_path = '/tmp/s3_aligned_cache'

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
    )

    cache_size = node.query(
        """
            SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize';
        """
    )
    assert int(cache_size) == node.get_cache_size(cache_path)

    node.restart_clickhouse()

    cache_size = node.query(
        """
            SELECT value FROM system.metrics WHERE name = 'FilesystemCacheSize';
        """
    )

    assert int(cache_size) == node.get_cache_size(cache_path)


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
