-- Tags: no-parallel-replicas
-- Test that a skip index whose expression wraps an analyzer-rewritten expression is used:
-- the filter constant must be pushed through the wrapping function to the index expression
-- even when the analyzer renames the filter expression (`multiIf` with a single condition
-- to `if`). Complements 05023_skip_index_analyzer_rewrites, which covers direct matches.
-- Regression test for issue #103128.
SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 1;
-- Disable statistics-based part pruning so that randomly injected
-- `auto_statistics_types` in CI does not add a Statistics section
-- to the EXPLAIN output and break the reference file.
SET use_statistics_for_part_pruning = 0;
-- The name of the plan step for the `WHERE` clause depends on the PREWHERE optimization
-- (`Expression` vs `Filter`), which CI randomizes, and it is irrelevant here, so the queries
-- below filter the plan step lines out of the `EXPLAIN` output and keep only the index analysis.
-- The remaining lines are also stripped of their leading indentation and plan-tree prefix, because
-- the nesting depth of `ReadFromMergeTree` depends on the same randomized settings.

DROP TABLE IF EXISTS test_skip_idx_rewrites_wrapped;

CREATE TABLE test_skip_idx_rewrites_wrapped
(
    t UInt32,
    v Int32,
    INDEX idx_monotonic (toInt64(multiIf(v > 0, v, NULL))) TYPE minmax GRANULARITY 1,
    INDEX idx_deterministic (cityHash64(multiIf(v > 0, v, 0))) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_idx_rewrites_wrapped (t, v)
SELECT number, number % 100 FROM numbers(100);

-- The index expression is a monotonic function (`toInt64`) of the rewritten expression:
-- the constant is pushed through it (the key-subexpression matching path).
SELECT 'monotonic_wrapper';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, NULL) > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- The index expression is a deterministic non-monotonic function (`cityHash64`) of the
-- rewritten expression: an equality constant is transformed into key space.
SELECT 'deterministic_wrapper';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, 0) = 98
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- The indexes must not change the results.
SELECT 'results';
SELECT count() FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, NULL) > 97;
SELECT count() FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, NULL) > 97 SETTINGS use_skip_indexes = 0;
SELECT count() FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, 0) = 98;
SELECT count() FROM test_skip_idx_rewrites_wrapped WHERE multiIf(v > 0, v, 0) = 98 SETTINGS use_skip_indexes = 0;

DROP TABLE test_skip_idx_rewrites_wrapped;
