-- Tags: no-random-settings, no-parallel-replicas

-- Regression test for a correctness gap found during review of the TopNAggregation
-- optimization: Mode 1 (sorted-input early termination) must apply the same
-- floating-point `NaN` guard as the standard read-in-order optimization. A `NaN`
-- sorts at one physical end of the MergeTree data, but `ORDER BY ... NULLS LAST`
-- expects it at the other end. A reverse read would surface the `NaN` group first, so
-- stopping after `K` groups could return the wrong top-K. Mode 1 must therefore be
-- rejected for a float determining column when `nulls_direction == -1`, falling back
-- to the standard pipeline (or Mode 2 threshold pruning), which handle `NaN` correctly.
-- `no-random-settings` keeps the EXPLAIN assertions stable.

DROP TABLE IF EXISTS t_topn_nan_asc;
DROP TABLE IF EXISTS t_topn_nan_desc;

-- (1) Float sorting key, ascending. `ORDER BY max(val) DESC NULLS LAST` needs a reverse
-- read, so Mode 1 must be rejected; results must still be correct.
CREATE TABLE t_topn_nan_asc (g UInt32, val Float64) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_topn_nan_asc SELECT number % 10, if(number % 10 = 0 AND number < 10, nan, toFloat64(number)) FROM numbers(1000);

SELECT '-- ASC float key: Mode 1 (sorted input) is NOT used';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT g, max(val) AS m FROM t_topn_nan_asc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 0
) WHERE explain LIKE '%Sorted input: true%';

SELECT '-- ASC float key: correct top-K (optimization on)';
SELECT g, max(val) AS m FROM t_topn_nan_asc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- ASC float key: matches reference (optimization off)';
SELECT g, max(val) AS m FROM t_topn_nan_asc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

-- (2) Float sorting key, descending (reverse key). Same guard via the reverse flag.
CREATE TABLE t_topn_nan_desc (g UInt32, val Float64)
ENGINE = MergeTree ORDER BY val DESC SETTINGS allow_experimental_reverse_key = 1;
INSERT INTO t_topn_nan_desc SELECT number % 10, if(number % 10 = 0 AND number < 10, nan, toFloat64(number)) FROM numbers(1000);

SELECT '-- DESC float key: Mode 1 (sorted input) is NOT used';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT g, max(val) AS m FROM t_topn_nan_desc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
    SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 0
) WHERE explain LIKE '%Sorted input: true%';

SELECT '-- DESC float key: correct top-K (optimization on)';
SELECT g, max(val) AS m FROM t_topn_nan_desc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- DESC float key: matches reference (optimization off)';
SELECT g, max(val) AS m FROM t_topn_nan_desc GROUP BY g ORDER BY m DESC NULLS LAST LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

DROP TABLE t_topn_nan_asc;
DROP TABLE t_topn_nan_desc;
