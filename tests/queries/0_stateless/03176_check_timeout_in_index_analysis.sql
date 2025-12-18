-- Tags: no-parallel, no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest
-- no-parallel because the test uses failpoint

CREATE TABLE t_03176(k UInt64, v UInt64) ENGINE=MergeTree() ORDER BY k PARTITION BY k;

INSERT INTO t_03176 SELECT number, number FROM numbers(5);

-- Table is partitioned by k to so it will have 5 partitions
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_03176' AND active;

-- This query is fast without failpoint: should take < 1 sec
EXPLAIN indexes = 1 SELECT * FROM t_03176 ORDER BY k LIMIT 5 SETTINGS log_comment = '03176_q1' FORMAT Null;

SYSTEM ENABLE FAILPOINT slowdown_index_analysis;

-- Check that failpont actually works: the query should take >= 5 sec
EXPLAIN indexes = 1 SELECT * FROM t_03176 ORDER BY k LIMIT 5 SETTINGS log_comment = '03176_q2' FORMAT Null;

-- Now the query should be cancelled after about 1 sec
EXPLAIN indexes = 1 SELECT * FROM t_03176 ORDER BY k LIMIT 5 SETTINGS log_comment = '03176_q3', max_execution_time = 1.1 FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SYSTEM DISABLE FAILPOINT slowdown_index_analysis;

SYSTEM FLUSH LOGS;

-- Check that q1 was fast, q2 was slow and q3 had timeout
SELECT log_comment, type = 'QueryFinish', intDiv(query_duration_ms, 2000), length(exception) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '03176_q_' AND type IN ('QueryFinish', 'ExceptionBeforeStart')
ORDER BY log_comment;

DROP TABLE t_03176;
