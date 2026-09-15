-- LIMIT still shrinks the block size with arrayJoin, only the source-side limit stays off (#82279)
DROP TABLE IF EXISTS t_aj_limit;
CREATE TABLE t_aj_limit (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_aj_limit SELECT number, [number] FROM numbers(20000);

SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 FORMAT Null SETTINGS log_comment = '05182_limit';

SYSTEM FLUSH LOGS query_log;
SELECT argMax(read_rows, event_time_microseconds) < 100 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_limit';

DROP TABLE t_aj_limit;
