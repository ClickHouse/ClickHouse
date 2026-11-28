-- Regression: `WITH CLUSTER` clusters the exact `GROUP BY` groups after aggregation, so it must
-- receive all of them. Anything that lets the aggregation emit only a subset of keys silently
-- produces a wrong cluster result. Two such paths are handled:
--   * `OptimizeTrivialGroupByLimitPass` (on by default) rewrites `GROUP BY ... LIMIT n` into
--     `max_rows_to_group_by = n, group_by_overflow_mode = 'any'`; it is now disabled for
--     `WITH CLUSTER` so the full grouping is preserved.
--   * an explicit non-throwing `max_rows_to_group_by` cap is rejected.
-- See https://github.com/ClickHouse/ClickHouse/pull/101878

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;
SET optimize_trivial_group_by_limit_query = 1;

-- Trivial `GROUP BY ... LIMIT` must NOT drop keys before clustering: 1 and 2 are within distance 1,
-- so they form a single cluster whose representative is the smaller key, 1 (not 2, the first key a
-- truncated aggregation would have kept).
SELECT 'trivial limit keeps full grouping';
SELECT x FROM VALUES('x UInt8', (2), (1)) GROUP BY x WITH CLUSTER 1 LIMIT 1;

-- An explicit non-throwing `max_rows_to_group_by` cap could truncate the grouping -> reject.
SELECT 'reject max_rows_to_group_by any';
SELECT x FROM VALUES('x UInt8', (2), (1)) GROUP BY x WITH CLUSTER 1
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any'; -- { serverError BAD_ARGUMENTS }

SELECT 'reject max_rows_to_group_by break';
SELECT x FROM VALUES('x UInt8', (2), (1)) GROUP BY x WITH CLUSTER 1
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break'; -- { serverError BAD_ARGUMENTS }

-- A `max_rows_to_group_by` cap in the default throwing mode does not truncate silently (it throws
-- only on actual overflow), so it stays allowed; here the two keys stay under the cap and cluster.
SELECT 'throw mode still allowed';
SELECT count() FROM (
    SELECT x FROM VALUES('x UInt8', (2), (1)) GROUP BY x WITH CLUSTER 1
    SETTINGS max_rows_to_group_by = 100
);
