-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

-- A primary key polluted with high cardinality values in its first column forces the generic
-- exclusion search to evaluate many small ranges. `merge_tree_generic_exclusion_search_max_steps` bounds the
-- work: a smaller budget selects more granules, and the selection converges to the exhaustive result
-- as the budget grows.

DROP TABLE IF EXISTS t_exclusion_search_max_steps;

CREATE TABLE t_exclusion_search_max_steps (a int, b int) ENGINE = MergeTree() ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_exclusion_search_max_steps
SELECT
  multiIf(number < 333, 1, number < 666, number, 1001), number
FROM numbers(1000);

OPTIMIZE TABLE t_exclusion_search_max_steps FINAL;

SET merge_tree_coarse_index_granularity = 8;

-- The reference pins the whole index analysis section, so use the legacy EXPLAIN layout, which has
-- no format-dependent summary lines.
SET explain_query_plan_default = 'legacy';

-- { echoOn }

SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_generic_exclusion_search_max_steps = 15
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';

SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_generic_exclusion_search_max_steps = 150
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';

SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_generic_exclusion_search_max_steps = 0
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Granules%' OR explain LIKE '%Keys%' OR explain LIKE '%Search Algorithm%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%' OR explain LIKE '%PrimaryKey%';

-- { echoOff }

DROP TABLE t_exclusion_search_max_steps;
