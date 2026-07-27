-- Tags: no-parallel-replicas
-- Test that skip indexes on ALIAS columns work even when query plan
-- expression merging is disabled, which prevents tryMergeExpressions
-- from composing filter and expression steps.
-- Regression test for issue #98822.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_alias_skip_idx;

CREATE TABLE test_alias_skip_idx
(
    c UInt32,
    a ALIAS c + 1,
    INDEX idx_a (a) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY c
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_alias_skip_idx SELECT number FROM numbers(10);
INSERT INTO test_alias_skip_idx SELECT number + 200 FROM numbers(10);

-- Test 1: Skip index used with merge_expressions disabled
SELECT 'merge_expressions=0';
EXPLAIN indexes = 1 SELECT * FROM test_alias_skip_idx WHERE a > 100
SETTINGS query_plan_merge_expressions = 0;

-- Test 2: Skip index used with all optimizations disabled
SELECT 'enable_optimizations=0';
EXPLAIN indexes = 1 SELECT * FROM test_alias_skip_idx WHERE a > 100
SETTINGS query_plan_enable_optimizations = 0;

-- Test 3: Nested ALIAS columns
DROP TABLE IF EXISTS test_nested_alias_idx;

CREATE TABLE test_nested_alias_idx
(
    c UInt32,
    a1 ALIAS c + 1,
    a2 ALIAS a1 + 1,
    INDEX idx_a2 (a2) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY c
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_nested_alias_idx SELECT number FROM numbers(10);
INSERT INTO test_nested_alias_idx SELECT number + 200 FROM numbers(10);

SELECT 'nested_alias';
EXPLAIN indexes = 1 SELECT * FROM test_nested_alias_idx WHERE a2 > 100
SETTINGS query_plan_merge_expressions = 0;

-- Test 4: Default settings still work (regression guard)
SELECT 'default_settings';
EXPLAIN indexes = 1 SELECT * FROM test_alias_skip_idx WHERE a > 100;

DROP TABLE test_alias_skip_idx;
DROP TABLE test_nested_alias_idx;
