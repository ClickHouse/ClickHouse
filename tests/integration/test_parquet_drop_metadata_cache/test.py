import pytest
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key
from helpers.config_cluster import minio_secret_key
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/config.d/cluster.xml"], with_zookeeper=True, with_minio=True)
node2 = cluster.add_instance("node2", main_configs=["configs/config.d/cluster.xml"], with_zookeeper=True)
node3 = cluster.add_instance("node3", main_configs=["configs/config.d/cluster.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_clear_cache_on_cluster(started_cluster):
    node1.query(f"INSERT INTO TABLE FUNCTION s3('http://minio1:9001/root/data/test_clear_cache/{{_partition_id}}.parquet', 'minio', '{minio_secret_key}', 'Parquet') PARTITION BY number SELECT number FROM numbers(1, 3)")

    node1.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/1.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cold_cache'")
    node2.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/2.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cold_cache'")
    node3.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/3.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cold_cache'")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER parquet_clear_cache_cluster")

    cold_cache_result_n1 = node1.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cold_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    cold_cache_result_n2 = node2.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cold_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    cold_cache_result_n3 = node3.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cold_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")

    assert(cold_cache_result_n1 == cold_cache_result_n2 == cold_cache_result_n3)
    assert(cold_cache_result_n1 == '0\n')

    node1.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/1.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='hot_cache'")
    node2.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/2.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='hot_cache'")
    node3.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/3.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='hot_cache'")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER parquet_clear_cache_cluster")

    warm_cache_result_n1 = node1.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'hot_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    warm_cache_result_n2 = node2.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'hot_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    warm_cache_result_n3 = node3.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'hot_cache' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")

    assert(warm_cache_result_n1 == warm_cache_result_n2 == warm_cache_result_n3)
    assert(warm_cache_result_n1 == '1\n')

    node1.query("SYSTEM DROP PARQUET METADATA CACHE ON CLUSTER parquet_clear_cache_cluster")

    node1.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/1.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cache_after_drop'")
    node2.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/2.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cache_after_drop'")
    node3.query(f"SELECT * FROM s3('http://minio1:9001/root/data/test_clear_cache/3.parquet', 'minio', '{minio_secret_key}', 'Parquet') SETTINGS log_comment='cache_after_drop'")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER parquet_clear_cache_cluster")

    cache_after_drop_result_n1 = node1.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    cache_after_drop_result_n2 = node2.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    cache_after_drop_result_n3 = node3.query("SELECT ProfileEvents['ParquetMetaDataCacheHits'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")

    assert(cache_after_drop_result_n1 == cache_after_drop_result_n2 == cache_after_drop_result_n3)
    assert(cache_after_drop_result_n1 == '0\n')

    misses_after_drop_result_n1 = node1.query("SELECT ProfileEvents['ParquetMetaDataCacheMisses'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    misses_after_drop_result_n2 = node2.query("SELECT ProfileEvents['ParquetMetaDataCacheMisses'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")
    misses_after_drop_result_n3 = node3.query("SELECT ProfileEvents['ParquetMetaDataCacheMisses'] FROM system.query_log where log_comment = 'cache_after_drop' AND type = 'QueryFinish' ORDER BY event_time desc LIMIT 1;")

    assert(misses_after_drop_result_n1 == misses_after_drop_result_n2 == misses_after_drop_result_n3)
    assert(misses_after_drop_result_n1 == '1\n')
