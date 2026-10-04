-- Tags: no-random-merge-tree-settings

-- An inner LIMIT cuts the prefix that an outer OFFSET sees, so the OFFSET-skip read-in-order optimization
-- must not drop leading granules below it unless the inner limit still covers `outer offset + outer limit`.
-- Otherwise the trimmed read promotes rows the inner limit would have dropped.

DROP TABLE IF EXISTS t_skip_offset_inner_limit;
CREATE TABLE t_skip_offset_inner_limit (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_skip_offset_inner_limit SELECT number FROM numbers(10);

SET optimize_read_in_order = 1, query_plan_optimize_read_in_order_skip_offset = 1;

SELECT 'inner limit does not cover the outer offset';
SELECT k FROM (SELECT k FROM t_skip_offset_inner_limit ORDER BY k LIMIT 2) ORDER BY k LIMIT 2 OFFSET 1;

SELECT 'inner limit covers the outer offset';
SELECT k FROM (SELECT k FROM t_skip_offset_inner_limit ORDER BY k LIMIT 8) ORDER BY k LIMIT 2 OFFSET 4;

SELECT 'inner limit above a pure offset';
SELECT k FROM (SELECT k FROM t_skip_offset_inner_limit ORDER BY k LIMIT 3) ORDER BY k OFFSET 1;

DROP TABLE t_skip_offset_inner_limit;
