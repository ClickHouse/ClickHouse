-- Tags: no-parallel, no-shared-merge-tree

DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;
DROP TABLE IF EXISTS t_prewarm_cache_rmt_2;

CREATE TABLE t_prewarm_cache_rmt_1 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '1')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

CREATE TABLE t_prewarm_cache_rmt_2 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '2')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

SYSTEM DROP MARK CACHE;

SYSTEM STOP FETCHES t_prewarm_cache_rmt_2;

-- Check that prewarm works on insert.
INSERT INTO t_prewarm_cache_rmt_1 SELECT number, rand(), rand() FROM numbers(20000);
SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

-- Check that prewarm works on fetch.
SYSTEM DROP MARK CACHE;
SYSTEM START FETCHES t_prewarm_cache_rmt_2;
SYSTEM SYNC REPLICA t_prewarm_cache_rmt_2;
SELECT count() FROM t_prewarm_cache_rmt_2 WHERE NOT ignore(*);

-- Check that prewarm works on merge.
INSERT INTO t_prewarm_cache_rmt_1 SELECT number, rand(), rand() FROM numbers(20000);
OPTIMIZE TABLE t_prewarm_cache_rmt_1 FINAL;

SYSTEM SYNC REPLICA t_prewarm_cache_rmt_2;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);
SELECT count() FROM t_prewarm_cache_rmt_2 WHERE NOT ignore(*);

-- Check that prewarm works on restart.
SYSTEM DROP MARK CACHE;

DETACH TABLE t_prewarm_cache_rmt_1;
DETACH TABLE t_prewarm_cache_rmt_2;

ATTACH TABLE t_prewarm_cache_rmt_1;
ATTACH TABLE t_prewarm_cache_rmt_2;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);
SELECT count() FROM t_prewarm_cache_rmt_2 WHERE NOT ignore(*);

SYSTEM DROP MARK CACHE;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

--- Check that system query works.
SYSTEM PREWARM MARK CACHE t_prewarm_cache_rmt_1;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['LoadedMarksCount'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM t_prewarm_cache%'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;
DROP TABLE IF EXISTS t_prewarm_cache_rmt_2;
