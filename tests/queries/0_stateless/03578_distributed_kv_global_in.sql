-- Tags: no-fasttest, use-rocksdb

SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS 03578_rocksdb_local, 03578_rocksdb_dist;

CREATE TABLE IF NOT EXISTS 03578_rocksdb_local
(
    key UInt64,
    val String
)
ENGINE = EmbeddedRocksDB()
PRIMARY KEY key;

CREATE TABLE IF NOT EXISTS 03578_rocksdb_dist
(
    key UInt64,
    val String
)
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 03578_rocksdb_local);

INSERT INTO 03578_rocksdb_local SELECT number, 'val-' || number FROM numbers(1000);

SELECT '-- RocksDB: set';

SELECT *
FROM 03578_rocksdb_dist
WHERE key GLOBAL IN (0, 1, 2)
ORDER BY 1, 2;

SELECT '-- RocksDB: subquery';

SELECT *
FROM 03578_rocksdb_dist
WHERE key GLOBAL IN (
    SELECT number FROM numbers(3)
)
ORDER BY 1, 2;

SYSTEM FLUSH LOGS query_log;

SELECT '-- Rows read:';

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%FROM 03578_rocksdb_dist%'
  AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE 03578_rocksdb_local, 03578_rocksdb_dist;

DROP TABLE IF EXISTS 03578_keepermap_local, 03578_keepermap_dist;

CREATE TABLE IF NOT EXISTS 03578_keepermap_local
(
    key UInt64,
    val String
)
ENGINE = KeeperMap('/' || currentDatabase() || '/test_03578_global_in')
PRIMARY KEY (key);

CREATE TABLE IF NOT EXISTS 03578_keepermap_dist
(
    key UInt64,
    val String
)
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), 03578_keepermap_local);

INSERT INTO 03578_keepermap_local SELECT number, 'val-' || number FROM numbers(1000);

SELECT '-- KeeperMap: set';

SELECT *
FROM 03578_keepermap_dist
WHERE key GLOBAL IN (0, 1, 2)
ORDER BY 1, 2;

SELECT '-- KeeperMap: subquery';

SELECT *
FROM 03578_keepermap_dist
WHERE key GLOBAL IN (
    SELECT number FROM numbers(3)
)
ORDER BY 1, 2;

SYSTEM FLUSH LOGS query_log;

SELECT '-- Rows read:';

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%FROM 03578_keepermap_dist%'
  AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS 03578_keepermap_local, 03578_keepermap_dist;
