-- Tags: no-parallel

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_prewarm_cache;

CREATE TABLE t_prewarm_cache (a UInt64, b UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03254_prewarm_mark_cache_smt/t_prewarm_cache', '1')
ORDER BY a SETTINGS prewarm_mark_cache = 0;

SYSTEM CLEAR MARK CACHE;

INSERT INTO t_prewarm_cache SELECT number, rand(), rand() FROM numbers(20000);

SELECT count() FROM t_prewarm_cache WHERE NOT ignore(*);

SYSTEM CLEAR MARK CACHE;

SYSTEM PREWARM MARK CACHE t_prewarm_cache;

SELECT count() FROM t_prewarm_cache WHERE NOT ignore(*);

SYSTEM FLUSH LOGS query_log;

-- With parallel replicas the local replica may read nothing; rows of remote replicas have current_database = 'default'.
SELECT sum(ProfileEvents['LoadedMarksCount']) > 0 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND initial_query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND query LIKE 'SELECT count() FROM t_prewarm_cache%'
)
GROUP BY initial_query_id
ORDER BY min(event_time_microseconds);

DROP TABLE IF EXISTS t_prewarm_cache;
