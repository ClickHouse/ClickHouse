-- A stateful outer filter must observe the rows produced by the inner filter.
-- `tryMergeFilters` (enabled by `query_plan_merge_filters`) used to collapse stacked `FilterStep`s
-- into one `and(...)` filter, evaluating the stateful predicate on the inner filter's input instead
-- of its output, so `neighbor` saw the unfiltered adjacent rows.

DROP TABLE IF EXISTS t_04621;
CREATE TABLE t_04621 (k UInt32, v UInt32) ENGINE = Memory;
INSERT INTO t_04621 SELECT number, number FROM numbers(10);

SET allow_deprecated_error_prone_window_functions = 1;
SET max_threads = 1;
SET max_block_size = 65536;
SET enable_parallel_replicas = 0;
SET query_plan_merge_filters = 1;

-- The inner filter keeps the even rows (v = 0, 2, 4, 6, 8), so `neighbor(v, 1) = v + 2` holds for
-- the first four of them. If the filters are merged, `neighbor` runs on the unfiltered stream where
-- the next value is always `v + 1`, and the count collapses to 0.
SELECT count() FROM (SELECT * FROM t_04621 WHERE k % 2 = 0) WHERE neighbor(v, 1) = v + 2 SETTINGS enable_analyzer = 1;
SELECT count() FROM (SELECT * FROM t_04621 WHERE k % 2 = 0) WHERE neighbor(v, 1) = v + 2 SETTINGS enable_analyzer = 0;

-- A stateful inner filter sees the same input rows whether or not the merge happens,
-- so it does not prevent the merge; the result must be correct either way.
SELECT count() FROM (SELECT * FROM t_04621 WHERE neighbor(v, 1) = v + 1) WHERE k % 2 = 0 SETTINGS enable_analyzer = 1;
SELECT count() FROM (SELECT * FROM t_04621 WHERE neighbor(v, 1) = v + 1) WHERE k % 2 = 0 SETTINGS enable_analyzer = 0;

DROP TABLE t_04621;
