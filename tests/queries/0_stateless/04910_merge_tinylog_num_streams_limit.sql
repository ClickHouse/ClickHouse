DROP TABLE IF EXISTS t_merge_tinylog_num_streams_limit;
DROP TABLE IF EXISTS t_tinylog_num_streams_limit;

CREATE TABLE t_tinylog_num_streams_limit (n UInt64) ENGINE = TinyLog;
INSERT INTO t_tinylog_num_streams_limit VALUES (1);

CREATE TABLE t_merge_tinylog_num_streams_limit (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_tinylog_num_streams_limit$');

SELECT count() FROM t_merge_tinylog_num_streams_limit
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1073741824;

DROP TABLE t_merge_tinylog_num_streams_limit;
DROP TABLE t_tinylog_num_streams_limit;
