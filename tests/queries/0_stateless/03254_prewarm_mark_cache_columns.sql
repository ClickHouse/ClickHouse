-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings

SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_prewarm_columns;

CREATE TABLE t_prewarm_columns (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 1, columns_to_prewarm_mark_cache = 'a,c';

INSERT INTO t_prewarm_columns VALUES (1, 1, 1, 1);

SELECT count() FROM t_prewarm_columns WHERE NOT ignore(*);

SYSTEM CLEAR MARK CACHE;
DETACH TABLE t_prewarm_columns;
ATTACH TABLE t_prewarm_columns;

SELECT count() FROM t_prewarm_columns WHERE NOT ignore(*);

SYSTEM CLEAR MARK CACHE;
SYSTEM PREWARM MARK CACHE t_prewarm_columns;

SELECT count() FROM t_prewarm_columns WHERE NOT ignore(*);

SYSTEM FLUSH LOGS query_log;

-- With parallel replicas the local replica may read nothing; rows of remote replicas have current_database = 'default'.
SELECT sum(ProfileEvents['LoadedMarksCount']) FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND initial_query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND query LIKE 'SELECT count() FROM t_prewarm_columns%'
)
GROUP BY initial_query_id
ORDER BY min(event_time_microseconds);

DROP TABLE t_prewarm_columns;
