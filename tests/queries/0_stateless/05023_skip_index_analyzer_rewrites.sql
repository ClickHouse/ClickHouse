-- Tags: no-parallel-replicas
-- Test that skip indexes on expressions are used when the analyzer rewrites
-- the filter expression (multiIf with a single condition to if, map element
-- access to a subcolumn read), including expressions coming from ALIAS columns.
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

DROP TABLE IF EXISTS test_skip_idx_rewrites;

CREATE TABLE test_skip_idx_rewrites
(
    t UInt32,
    attrs Array(LowCardinality(String)),
    m Map(LowCardinality(String), LowCardinality(String)),
    v Int32,
    a Nullable(Int32) ALIAS if(has(attrs, 'a'), multiIf((m['a']) = 'v', v, NULL), NULL),
    INDEX idx_a (if(has(attrs, 'a'), multiIf((m['a']) = 'v', v, NULL), NULL)) TYPE minmax GRANULARITY 1,
    INDEX idx_multiif (multiIf(v > 0, v, NULL)) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_idx_rewrites (t, attrs, m, v)
SELECT number, ['a'], map('a', 'v'), number % 100 FROM numbers(100);

-- Test 1: the issue's case: a filter on an ALIAS column whose expression is
-- rewritten by the analyzer (multiIf -> if, map element -> subcolumn).
SELECT 'alias_column';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT t, a FROM test_skip_idx_rewrites WHERE a > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- Test 2: the same expression written out verbatim.
SELECT 'verbatim_expression';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites
WHERE if(has(attrs, 'a'), multiIf((m['a']) = 'v', v, NULL), NULL) > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- Test 3: multiIf rewritten to if by optimize_multiif_to_if.
SELECT 'multiif_to_if';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites WHERE multiIf(v > 0, v, NULL) > 97
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- Test 4: the rewrites disabled: the index is matched by the original names.
SELECT 'rewrites_disabled';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT t FROM test_skip_idx_rewrites WHERE multiIf(v > 0, v, NULL) > 97
SETTINGS optimize_multiif_to_if = 0
) WHERE explain NOT LIKE '%Expression (%' AND explain NOT LIKE '%Filter (%';

-- The index must not change the result.
SELECT 'results';
SELECT count() FROM test_skip_idx_rewrites WHERE a > 97;
SELECT count() FROM test_skip_idx_rewrites WHERE a > 97 SETTINGS use_skip_indexes = 0;

DROP TABLE test_skip_idx_rewrites;
