-- With arrayJoin a small LIMIT still shrinks the block size (only the hard source limit stays off, see 04320).
DROP TABLE IF EXISTS t_aj_limit;
CREATE TABLE t_aj_limit (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_aj_limit SELECT number, [number] FROM numbers(20000);

SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 FORMAT Null SETTINGS enable_analyzer = 1, log_comment = '05182_analyzer';
SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 FORMAT Null SETTINGS enable_analyzer = 0, log_comment = '05182_old';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMax(read_rows, event_time_microseconds) < 100 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05182_%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_aj_limit;
