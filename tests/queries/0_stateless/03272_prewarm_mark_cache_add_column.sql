-- Tags: no-parallel
-- no-parallel: SYSTEM DROP MARK CACHE is used.

DROP TABLE IF EXISTS t_prewarm_add_column;

CREATE TABLE t_prewarm_add_column (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS prewarm_mark_cache = 1, min_bytes_for_wide_part = 0;

-- Drop mark cache because it may be full and we will fail to add new entries to it.
SYSTEM DROP MARK CACHE;
SYSTEM STOP MERGES t_prewarm_add_column;

INSERT INTO t_prewarm_add_column VALUES (1);

ALTER TABLE t_prewarm_add_column ADD COLUMN b UInt64;

INSERT INTO t_prewarm_add_column VALUES (2, 2);

DETACH TABLE t_prewarm_add_column;
ATTACH TABLE t_prewarm_add_column;

SELECT * FROM t_prewarm_add_column ORDER BY a;
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['LoadedMarksCount'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT * FROM t_prewarm_add_column%'
ORDER BY event_time_microseconds;

DROP TABLE t_prewarm_add_column;
