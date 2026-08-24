DROP TABLE IF EXISTS t_group_concat_overload;
CREATE TABLE t_group_concat_overload (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Use identical rows so the output is deterministic regardless of how the flaky check
-- randomises threads/parts/parallel-replicas. The test still exercises the limit and
-- the delimiter override.
INSERT INTO t_group_concat_overload SELECT 42 FROM numbers(5);

SELECT 'baseline:', groupConcat(',', 2)(x) FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'limit kept:', groupConcat(',', 2)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'delim overridden:', groupConcat(',', 3)(x, '|') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;
SELECT 'large limit:', groupConcat(',', 100)(x, '/') FROM t_group_concat_overload SETTINGS enable_analyzer = 1;

DROP TABLE t_group_concat_overload;
