-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest

CREATE TABLE t_03176(k UInt64, v UInt64) ENGINE=MergeTree() ORDER BY k PARTITION BY k;

INSERT INTO t_03176 SELECT number, number FROM numbers(5);

-- Table is partitioned by k to so it will have 5 partitions
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_03176' AND active;

-- This query is fast without failpoint: should take < 1 sec
EXPLAIN indexes = 1 SELECT * FROM t_03176 ORDER BY k LIMIT 5 SETTINGS log_comment = '03176_q1' FORMAT Null;

-- Now the query should be cancelled
EXPLAIN indexes = 1 SELECT * FROM t_03176 ORDER BY k LIMIT 5 SETTINGS log_comment = '03176_q3', max_execution_time = 0.00001 FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;

-- Check that q1 was fast, q2 was slow and q3 had timeout
SELECT log_comment, type = 'QueryFinish', intDiv(query_duration_ms, 2000), exception_code != 0, (position('selectPartsToRead' IN stack_trace) > 0 OR position('filterPartsByPartition' IN stack_trace) > 0) AS has_selectPartsToRead
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '03176_q_' AND type IN ('QueryFinish', 'ExceptionBeforeStart')
ORDER BY log_comment;

DROP TABLE t_03176;

