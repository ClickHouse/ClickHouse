-- LIMIT still shrinks the block size with arrayJoin, only the source-side limit stays off (#82279)
DROP TABLE IF EXISTS t_aj_limit;
CREATE TABLE t_aj_limit (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_aj_limit SELECT number, [number] FROM numbers(20000);

SELECT arrayJoin(a) FROM t_aj_limit LIMIT 1 FORMAT Null SETTINGS max_threads = 8, log_comment = '05182_limit';

SYSTEM FLUSH LOGS query_log;
SELECT argMax(read_rows, event_time_microseconds) < 100 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_limit';

DROP TABLE t_aj_limit;

-- a long empty-array prefix: the LIMIT is tiny but the source has to get past the prefix
DROP TABLE IF EXISTS t_aj_sparse;
CREATE TABLE t_aj_sparse (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_aj_sparse SELECT number, if(number < 9000, [], [number]) FROM numbers(10000);

SELECT arrayJoin(a) FROM t_aj_sparse LIMIT 3 SETTINGS max_threads = 1, log_comment = '05182_sparse';

SYSTEM FLUSH LOGS query_log;
SELECT argMax(read_rows, event_time_microseconds) >= 9000 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05182_sparse';

DROP TABLE t_aj_sparse;
