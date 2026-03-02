-- Tags: no-parallel, no-shared-merge-tree, no-parallel-replicas

DROP TABLE IF EXISTS t_prewarm_idx_cache_1;
DROP TABLE IF EXISTS t_prewarm_idx_cache_2;

CREATE TABLE t_prewarm_idx_cache_1 (a UInt64, b UInt64, c String, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03985_prewarm_index_mark_cache/t', '1')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

CREATE TABLE t_prewarm_idx_cache_2 (a UInt64, b UInt64, c String, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03985_prewarm_index_mark_cache/t', '2')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;

SYSTEM STOP FETCHES t_prewarm_idx_cache_2;

-- Check that prewarm works on insert.
INSERT INTO t_prewarm_idx_cache_1 SELECT number, number, randomString(10) FROM numbers(20000);
SELECT count() FROM t_prewarm_idx_cache_1 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'prewarm on insert';

-- Check that prewarm works on fetch.
SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;

SYSTEM START FETCHES t_prewarm_idx_cache_2;
SYSTEM SYNC REPLICA t_prewarm_idx_cache_2;
SELECT count() FROM t_prewarm_idx_cache_2 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'prewarm on fetch';

-- Check that prewarm works on merge.
INSERT INTO t_prewarm_idx_cache_1 SELECT number, number, randomString(10) FROM numbers(20000);
OPTIMIZE TABLE t_prewarm_idx_cache_1 FINAL;

SYSTEM SYNC REPLICA t_prewarm_idx_cache_2;

SELECT count() FROM t_prewarm_idx_cache_1 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after prewarm on merge';
SELECT count() FROM t_prewarm_idx_cache_2 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after prewarm on merge';

-- Check that prewarm works on restart.
SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;

DETACH TABLE t_prewarm_idx_cache_1;
DETACH TABLE t_prewarm_idx_cache_2;

ATTACH TABLE t_prewarm_idx_cache_1;
ATTACH TABLE t_prewarm_idx_cache_2;

SELECT count() FROM t_prewarm_idx_cache_1 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after prewarm on restart';
SELECT count() FROM t_prewarm_idx_cache_2 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after prewarm on restart';

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;

SELECT count() FROM t_prewarm_idx_cache_1 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after drop mark cache';

--- Check that system query works.
SYSTEM PREWARM MARK CACHE t_prewarm_idx_cache_1;

SELECT count() FROM t_prewarm_idx_cache_1 WHERE b < 10000 AND NOT ignore(*) SETTINGS log_comment = 'after prewarm with system query';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['LoadedMarksCount'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM t_prewarm_idx_cache%'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_prewarm_idx_cache_1;
DROP TABLE IF EXISTS t_prewarm_idx_cache_2;
