-- Tags: no-parallel, no-fasttest
-- no-parallel, no-fasttest: enables a global failpoint.

DROP TABLE IF EXISTS t;

CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t;

INSERT INTO t SETTINGS async_insert = 0 VALUES (1, 10);
INSERT INTO t SETTINGS async_insert = 0 VALUES (2, 20);
INSERT INTO t SETTINGS async_insert = 0 VALUES (3, 30);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;

SYSTEM ENABLE FAILPOINT slowdown_index_analysis_per_part;

SELECT name, enabled FROM system.fail_points WHERE name = 'slowdown_index_analysis_per_part';

-- With `lock_acquire_timeout = 1` a regression fails fast instead of after the default timeout.
SELECT sum(v) FROM t WHERE k > 0 SETTINGS max_threads = 2, lock_acquire_timeout = 1, use_query_condition_cache = 0, log_comment = '05045_stalled';

SYSTEM DISABLE FAILPOINT slowdown_index_analysis_per_part;

SELECT sum(v) FROM t WHERE k > 0 SETTINGS max_threads = 2, lock_acquire_timeout = 1, use_query_condition_cache = 0;

-- The failpoint really delayed the analysis above (a lower bound, so a slow runner cannot break it).
SYSTEM FLUSH LOGS query_log;
SELECT count(), min(query_duration_ms) >= 3000 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05045_stalled';

DROP TABLE t;
