-- The step budget of the generic exclusion search may only change which granules are read, never the
-- query results: ranges that were not fully analyzed are accepted as a whole and filtered on read.

DROP TABLE IF EXISTS t_exclusion_steps_correct;

CREATE TABLE t_exclusion_steps_correct (a int, b int) ENGINE = MergeTree() ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_exclusion_steps_correct
SELECT
  multiIf(number < 333, 1, number < 666, number, 1001), number
FROM numbers(1000);

OPTIMIZE TABLE t_exclusion_steps_correct FINAL;

SET merge_tree_coarse_index_granularity = 8;

-- The exact-count optimization is what consumes the exact ranges, so it must be enabled for the
-- exact-range queries below to exercise them.
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

-- { echoOn }

SELECT count(), sum(cityHash64(a, b)) FROM t_exclusion_steps_correct WHERE b < 50 OR b >= 950
SETTINGS merge_tree_generic_exclusion_search_max_steps = 1;
SELECT count(), sum(cityHash64(a, b)) FROM t_exclusion_steps_correct WHERE b < 50 OR b >= 950
SETTINGS merge_tree_generic_exclusion_search_max_steps = 15;
SELECT count(), sum(cityHash64(a, b)) FROM t_exclusion_steps_correct WHERE b < 50 OR b >= 950
SETTINGS merge_tree_generic_exclusion_search_max_steps = 100000;
SELECT count(), sum(cityHash64(a, b)) FROM t_exclusion_steps_correct WHERE b < 50 OR b >= 950
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0;

-- The exact-count optimization consumes the exact ranges produced by the search, so their contract
-- (sorted, and every row matches) must hold under a step budget too. The two matching runs lie
-- closer together than the seek threshold, so the ranges to read are merged across the gap while the
-- exact ranges must not be.
SELECT count() FROM t_exclusion_steps_correct WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23)
SETTINGS merge_tree_generic_exclusion_search_max_steps = 5, merge_tree_min_rows_for_seek = 7;
SELECT count() FROM t_exclusion_steps_correct WHERE (b BETWEEN 10 AND 13) OR (b BETWEEN 20 AND 23)
SETTINGS merge_tree_generic_exclusion_search_max_steps = 50, merge_tree_min_rows_for_seek = 7;

-- The query condition cache can split a part into several ranges before the primary key analysis
-- runs, so the search receives multiple initial ranges. The cache is populated only by a query that
-- actually reads and filters, so the first query disables the exact-count optimization.
SELECT count() FROM t_exclusion_steps_correct WHERE b = 5
SETTINGS use_query_condition_cache = 1, optimize_use_projections = 0;
SELECT count() FROM t_exclusion_steps_correct WHERE b = 5
SETTINGS use_query_condition_cache = 1, merge_tree_generic_exclusion_search_max_steps = 15;

-- { echoOff }

DROP TABLE t_exclusion_steps_correct;
