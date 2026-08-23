-- A stream count that is an exact multiple of 2^32 used to arrive at the reader as zero and reach a
-- division by it. If a `Merge` stream-count limit lands upstream first these become
-- PARAMETER_OUT_OF_BOUND, and the expectations below have to be updated then.
DROP TABLE IF EXISTS t_05038;
CREATE TABLE t_05038 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05038 SELECT number FROM numbers(100000);

-- Read-in-order stream spreading.
SELECT a FROM merge(currentDatabase(), '^t_05038$') ORDER BY a DESC LIMIT 3
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648, optimize_read_in_order = 1;

-- A full value, not a row count: a trivial-count plan can elide the read step entirely.
SELECT sum(a) FROM merge(currentDatabase(), '^t_05038$')
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648;

DROP TABLE IF EXISTS t_final_05038;
CREATE TABLE t_final_05038 (a UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY a;
INSERT INTO t_final_05038 SELECT number, 1 FROM numbers(100000);

-- The FINAL spreading path asserts that its local stream count still equals the step's requested one.
SELECT sum(a) FROM merge(currentDatabase(), '^t_final_05038$') FINAL
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648;

DROP TABLE t_final_05038;
DROP TABLE t_05038;
