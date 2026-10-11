-- Tags: no-shared-merge-tree, no-parallel
-- Tag no-parallel: uses shared cache state and must remain isolated from concurrent cache tests.

DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;
DROP TABLE IF EXISTS t_prewarm_cache_rmt_2;

CREATE TABLE t_prewarm_cache_rmt_1 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '1')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

CREATE TABLE t_prewarm_cache_rmt_2 (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '2')
ORDER BY a SETTINGS prewarm_mark_cache = 1;

SYSTEM CLEAR MARK CACHE;
SYSTEM CLEAR MARK CACHE;

SYSTEM STOP FETCHES t_prewarm_cache_rmt_2;

-- Check that prewarm works on insert.
INSERT INTO t_prewarm_cache_rmt_1 SELECT number, rand(), rand() FROM numbers(20000);
SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

-- Check that prewarm works on fetch.
SYSTEM CLEAR MARK CACHE;
SYSTEM CLEAR MARK CACHE;
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
SYSTEM CLEAR MARK CACHE;
SYSTEM CLEAR MARK CACHE;

DETACH TABLE t_prewarm_cache_rmt_1;
DETACH TABLE t_prewarm_cache_rmt_2;

ATTACH TABLE t_prewarm_cache_rmt_1;
ATTACH TABLE t_prewarm_cache_rmt_2;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);
SELECT count() FROM t_prewarm_cache_rmt_2 WHERE NOT ignore(*);

SYSTEM CLEAR MARK CACHE;
SYSTEM CLEAR MARK CACHE;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

--- Check that system query works.
SET log_comment = '03254_prewarm_mark_cache_rmt_final';
SYSTEM PREWARM MARK CACHE t_prewarm_cache_rmt_1;

SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);
SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);
SELECT count() FROM t_prewarm_cache_rmt_1 WHERE NOT ignore(*);

SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

-- The mark cache is process-wide and can be evicted by another test. Repeated
-- selects make the assertion cover the accumulated evidence after prewarming.
-- With parallel replicas the local replica may read nothing; rows of remote replicas have current_database = 'default'.
SELECT countIf(loaded_marks = 0) > 0 FROM
(
    SELECT sum(ProfileEvents['LoadedMarksCount']) AS loaded_marks FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND initial_query_id IN
    (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment = '03254_prewarm_mark_cache_rmt_final' AND query LIKE 'SELECT count() FROM t_prewarm_cache%'
    )
    GROUP BY initial_query_id
);

DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;
DROP TABLE IF EXISTS t_prewarm_cache_rmt_2;
