-- Tags: no-parallel

DROP TABLE IF EXISTS t_prewarm_cache;

CREATE TABLE t_prewarm_cache (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '1')
ORDER BY a SETTINGS prewarm_mark_cache = 0;

SYSTEM DROP MARK CACHE;

INSERT INTO t_prewarm_cache SELECT number, rand(), rand() FROM numbers(20000);

SELECT count() FROM t_prewarm_cache WHERE NOT ignore(*);

SYSTEM DROP MARK CACHE;

SYSTEM PREWARM MARK CACHE t_prewarm_cache;

SELECT count() FROM t_prewarm_cache WHERE NOT ignore(*);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['LoadedMarksCount'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM t_prewarm_cache%'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_prewarm_cache;
