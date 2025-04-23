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
    
        
