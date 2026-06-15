-- Tags: no-random-settings, no-parallel-replicas

-- Regression test for a correctness gap found during review of the TopNAggregation
-- optimization: Mode 2 threshold pruning must be rejected for floating-point determining
-- columns when the query orders `NULLS FIRST` (`nulls_direction != direction`). The
-- `min`/`max` update rule ranks `NaN` at the worst end (any numeric value replaces a `NaN`
-- accumulator — see `SingleValueDataFixed::setIfSmaller`/`setIfGreater`), while `NULLS FIRST`
-- ordering ranks `NaN` best. Once the K-th boundary becomes `NaN`, every numeric row is
-- "below the boundary" and would be pruned — including rows that should replace an existing
-- group's `NaN` state, so the group would incorrectly keep `NaN`. Level 2 would publish the
-- same bad boundary to the `__topKFilter` prewhere.
-- `no-random-settings` keeps the EXPLAIN assertions stable.

SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_topn_nan_mode2;
-- Sorting key differs from the aggregate argument, so only Mode 2 can apply.
CREATE TABLE t_topn_nan_mode2 (g UInt32, val Float64) ENGINE = MergeTree ORDER BY g;

-- The first part gives several groups a `NaN`-only state; the second part delivers the
-- numeric values in later blocks, reproducing the "group receives `NaN` first and a numeric
-- value later" shape from the review finding.
INSERT INTO t_topn_nan_mode2 SELECT number, nan FROM numbers(5);
INSERT INTO t_topn_nan_mode2 SELECT number % 10, toFloat64(number) FROM numbers(1000);

SELECT '-- max DESC NULLS FIRST: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m DESC NULLS FIRST LIMIT 3
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- max DESC NULLS FIRST: correct top-K (optimization on)';
SELECT g, max(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m DESC NULLS FIRST LIMIT 3
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2;

SELECT '-- max DESC NULLS FIRST: matches reference (optimization off)';
SELECT g, max(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m DESC NULLS FIRST LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

SELECT '-- max DESC NULLS LAST: has TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, max(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- max DESC NULLS LAST: correct top-K (optimization on)';
SELECT g, max(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2;

SELECT '-- min ASC NULLS FIRST: no TopNAggregating';
SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT g, min(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m ASC NULLS FIRST LIMIT 3
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2
) WHERE explain LIKE '%TopNAggregating%';

SELECT '-- min ASC NULLS FIRST: correct top-K (optimization on)';
SELECT g, min(val) AS m FROM t_topn_nan_mode2 GROUP BY g ORDER BY m ASC NULLS FIRST LIMIT 3
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2;

DROP TABLE t_topn_nan_mode2;
