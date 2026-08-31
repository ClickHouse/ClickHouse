-- Tags: no-parallel-replicas
-- Test that skip indexes on expressions are used when the legacy analyzer rewrites the filter
-- expression: `TreeOptimizer::optimizeIf` rewrites a `multiIf` with a single condition to `if`
-- (`optimize_multiif_to_if`), so the filter expression is named differently than the index
-- expression. Regression test for issue #103128, the `enable_analyzer = 0` counterpart of
-- `05023_skip_index_analyzer_rewrites`.
SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 0;
-- Disable statistics-based part pruning so that randomly injected
-- `auto_statistics_types` in CI does not add a Statistics section
-- to the EXPLAIN output and break the reference file.
SET use_statistics_for_part_pruning = 0;
-- The name of the plan step for the `WHERE` clause depends on the PREWHERE optimization
-- (`Expression` vs `Filter`), which CI randomizes, and it is irrelevant here, so the queries
-- below filter the plan step lines out of the `EXPLAIN` output and keep only the index analysis.
-- The remaining lines are also stripped of their leading indentation and plan-tree prefix, because
-- the nesting depth of `ReadFromMergeTree` depends on the same randomized settings.

DROP TABLE IF EXISTS test_skip_idx_rewrites_legacy;

CREATE TABLE test_skip_idx_rewrites_legacy
(
    t UInt32,
    v Int32,
    INDEX idx_multiif (multiIf(v > 0, v, NULL)) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_idx_rewrites_legacy SELECT number, number % 100 FROM numbers(100);

-- `multiIf` in the filter is rewritten to `if` by `optimize_multiif_to_if`, the index expression is not.
-- The setting is set for the whole session: with the legacy analyzer, a `SETTINGS` clause of the
-- subquery is not applied to the analysis of the `EXPLAIN`ed query.
SET optimize_multiif_to_if = 1;
SELECT 'multiif_to_if';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites_legacy WHERE multiIf(v > 0, v, NULL) > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- The rewrite disabled: the index is matched by the original names.
SET optimize_multiif_to_if = 0;
SELECT 'rewrites_disabled';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites_legacy WHERE multiIf(v > 0, v, NULL) > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';
SET optimize_multiif_to_if = 1;

-- The index must not change the result.
SELECT 'results';
SELECT count() FROM test_skip_idx_rewrites_legacy WHERE multiIf(v > 0, v, NULL) > 97;
SELECT count() FROM test_skip_idx_rewrites_legacy WHERE multiIf(v > 0, v, NULL) > 97 SETTINGS use_skip_indexes = 0;

DROP TABLE test_skip_idx_rewrites_legacy;
