DROP TABLE IF EXISTS t_05037;
CREATE TABLE t_05037 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05037 SELECT number FROM numbers(100000);

-- A direct read is floored while the query is planned, so it must be unaffected at every ratio.
SELECT a FROM t_05037 ORDER BY a DESC LIMIT 3
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648, optimize_read_in_order = 1;
SELECT sum(a) FROM t_05037
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648;

-- A `merge()` read at a ratio that leaves the stream count intact must also be unaffected.
SELECT a FROM merge(currentDatabase(), '^t_05037$') ORDER BY a DESC LIMIT 3
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1073741824, optimize_read_in_order = 1;
SELECT sum(a) FROM merge(currentDatabase(), '^t_05037$')
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1073741824;

DROP TABLE t_05037;
