-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- Tag no-fasttest: Depends on AWS/MinIO paimon_all_types dataset
-- Tag no-parallel: cache is system-wide and tests can affect each other
-- Tag no-parallel-replicas: profile events are not available on the second replica
-- Tag no-random-settings: we need to test specific cache setting combinations

SET allow_experimental_paimon_storage_engine = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS paimon_cache_off;
DROP TABLE IF EXISTS paimon_cache_on;

SYSTEM DROP PAIMON METADATA CACHE;

-- ============ Cache OFF: neither hit nor miss ============
-- use_paimon_metadata_files_cache defaults to false, so PaimonMetadata::create()
-- will set cache_ptr = nullptr and no ProfileEvents will be recorded.
CREATE TABLE paimon_cache_off
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

SELECT count() FROM paimon_cache_off
SETTINGS log_comment = '04209-cache-off-1'
FORMAT Null;

SELECT count() FROM paimon_cache_off
SETTINGS log_comment = '04209-cache-off-2'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Expected: 0 0 (cache disabled at table creation time, no events recorded)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'],
    ProfileEvents['PaimonMetadataFilesCacheMisses']
FROM system.query_log
WHERE log_comment = '04209-cache-off-2'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

DROP TABLE paimon_cache_off;

-- ============ Cache ON: first query miss, second query hit ============
-- IMPORTANT: use_paimon_metadata_files_cache must be set at session level BEFORE
-- CREATE TABLE, because PaimonMetadata::create() reads this setting to initialize
-- cache_ptr in PaimonPersistentComponents. Per-query SETTINGS in SELECT has no
-- effect on an already-created metadata object (supportsUpdate path).
SET use_paimon_metadata_files_cache = 1;

CREATE TABLE paimon_cache_on
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

SELECT count() FROM paimon_cache_on
SETTINGS log_comment = '04209-cache-on-miss'
FORMAT Null;

SELECT count() FROM paimon_cache_on
SETTINGS log_comment = '04209-cache-on-hit'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Expected: 1 1 (first query: hits == 0 AND misses > 0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04209-cache-on-miss'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- Expected: 1 1 (second query: hits > 0 AND misses == 0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] > 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = '04209-cache-on-hit'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

DROP TABLE paimon_cache_on;

SYSTEM DROP PAIMON METADATA CACHE;

-- ============ Table Function: cache ON, miss then hit ============
-- Table functions create a fresh PaimonMetadata per query, but bind
-- the same global cache with deterministic cache keys. Verify that
-- the second paimonS3() call hits the cache populated by the first.

SELECT count() FROM paimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS use_paimon_metadata_files_cache = 1, log_comment = '04209-tf-miss'
FORMAT Null;

SELECT count() FROM paimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS use_paimon_metadata_files_cache = 1, log_comment = '04209-tf-hit'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Expected: 1 1 (first query: hits == 0 AND misses > 0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04209-tf-miss'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- Expected: 1 1 (second query: hits > 0 AND misses == 0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] > 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = '04209-tf-hit'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

SYSTEM DROP PAIMON METADATA CACHE;
