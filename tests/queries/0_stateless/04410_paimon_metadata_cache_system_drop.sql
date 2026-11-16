-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- Tag no-fasttest: Depends on AWS/MinIO paimon_all_types dataset
-- Tag no-parallel: cache is system-wide and tests can affect each other
-- Tag no-parallel-replicas: profile events are not available on the second replica
-- Tag no-random-settings: we need to test specific cache setting combinations

SET allow_experimental_paimon_storage_engine = 1;
SET use_paimon_metadata_files_cache = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS paimon_sys_drop_t1;
DROP TABLE IF EXISTS paimon_sys_drop_t2;
SYSTEM DROP PAIMON METADATA CACHE;

-- ============ Scenario 1: SYSTEM DROP clears cache ============
-- Create table, populate cache, SYSTEM DROP, verify next query misses.

CREATE TABLE paimon_sys_drop_t1
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

-- q1: miss (cold cache)
SELECT count() FROM paimon_sys_drop_t1
SETTINGS log_comment = '04410-q1-miss'
FORMAT Null;

-- q2: hit (warm cache)
SELECT count() FROM paimon_sys_drop_t1
SETTINGS log_comment = '04410-q2-hit'
FORMAT Null;

-- Clear the global cache
SYSTEM DROP PAIMON METADATA CACHE;

-- q3: miss again (proves SYSTEM DROP worked)
SELECT count() FROM paimon_sys_drop_t1
SETTINGS log_comment = '04410-q3-after-drop'
FORMAT Null;

DROP TABLE paimon_sys_drop_t1;

-- ============ Scenario 2: Same-named table re-CREATE shares cache safely ============
-- DROP + re-CREATE a table with the same name over the same path.
-- Since schema-0 timeMillis is unchanged, cache key is identical => valid reuse.
-- This confirms cache is not "mixed up" — it is deterministically keyed.

SYSTEM DROP PAIMON METADATA CACHE;

CREATE TABLE paimon_sys_drop_t2
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

-- q4: miss (fresh cache after SYSTEM DROP)
SELECT count() FROM paimon_sys_drop_t2
SETTINGS log_comment = '04410-q4-first-create'
FORMAT Null;

-- q5: hit (confirms cache populated)
SELECT count() FROM paimon_sys_drop_t2
SETTINGS log_comment = '04410-q5-hit-verify'
FORMAT Null;

-- Drop and re-create with same name and same path
DROP TABLE paimon_sys_drop_t2;
CREATE TABLE paimon_sys_drop_t2
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types');

-- q6: hit (cache key unchanged because schema-0 is the same)
SELECT count() FROM paimon_sys_drop_t2
SETTINGS log_comment = '04410-q6-recreate-hit'
FORMAT Null;

-- q7: after SYSTEM DROP, proves cache was the causal factor
SYSTEM DROP PAIMON METADATA CACHE;
SELECT count() FROM paimon_sys_drop_t2
SETTINGS log_comment = '04410-q7-final-miss'
FORMAT Null;

DROP TABLE paimon_sys_drop_t2;

-- ============ Verify all results via query_log ============
SYSTEM FLUSH LOGS query_log;

-- q1: miss (hits=0, misses>0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04410-q1-miss'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q2: hit (hits>0, misses=0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] > 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = '04410-q2-hit'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q3: miss after SYSTEM DROP (hits=0, misses>0)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04410-q3-after-drop'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q4: miss (fresh cache)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04410-q4-first-create'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q5: hit
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] > 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = '04410-q5-hit-verify'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q6: hit after re-CREATE (cache key unchanged, valid reuse)
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] > 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = '04410-q6-recreate-hit'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();

-- q7: miss after final SYSTEM DROP
SELECT
    ProfileEvents['PaimonMetadataFilesCacheHits'] = 0,
    ProfileEvents['PaimonMetadataFilesCacheMisses'] > 0
FROM system.query_log
WHERE log_comment = '04410-q7-final-miss'
  AND type = 'QueryFinish'
  AND current_database = currentDatabase();
