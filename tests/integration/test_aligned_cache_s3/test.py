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
