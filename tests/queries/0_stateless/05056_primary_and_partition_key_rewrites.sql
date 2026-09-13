-- Tags: no-parallel-replicas
-- Test that a primary key and a partition key on expressions are used for pruning when the query
-- analyzer rewrites the filter expression: `optimize_multiif_to_if` rewrites a `multiIf` with a
-- single condition to `if`, so the filter expression is named differently than the key expression.
-- The counterpart of `05023_skip_index_analyzer_rewrites` for the primary and partition keys.
SET explain_query_plan_default = 'legacy';

-- The `EXPLAIN` output is not compared verbatim: the shape of the plan and the number of granules
-- left after the analysis depend on settings CI randomizes. Only the condition the key analysis
-- derived is checked: without the rewrite-aware matching it is `true` (the key is not used).

DROP TABLE IF EXISTS test_primary_key_rewrites;

CREATE TABLE test_primary_key_rewrites (t UInt32, v Int32)
ENGINE = MergeTree
ORDER BY multiIf(v > 0, v, 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_primary_key_rewrites SELECT number, number % 100 FROM numbers(100);

SELECT 'primary_key', countIf(explain LIKE '%Condition: (multiIf(greater(v, 0), v, 0) in [98, +Inf))%')
FROM (EXPLAIN indexes = 1 SELECT t FROM test_primary_key_rewrites WHERE multiIf(v > 0, v, 0) > 97);

SET enable_analyzer = 0;
SELECT 'primary_key_legacy_analyzer', countIf(explain LIKE '%Condition: (multiIf(greater(v, 0), v, 0) in [98, +Inf))%')
FROM (EXPLAIN indexes = 1 SELECT t FROM test_primary_key_rewrites WHERE multiIf(v > 0, v, 0) > 97);
SET enable_analyzer = 1;

-- The key must not change the result.
SELECT 'primary_key_results';
SELECT count() FROM test_primary_key_rewrites WHERE multiIf(v > 0, v, 0) > 97;
SELECT count() FROM test_primary_key_rewrites WHERE multiIf(v > 0, v, 0) > 97 SETTINGS use_primary_key = 0;

DROP TABLE test_primary_key_rewrites;

DROP TABLE IF EXISTS test_partition_key_rewrites;

CREATE TABLE test_partition_key_rewrites (t UInt32, v Int32)
ENGINE = MergeTree
ORDER BY t
PARTITION BY multiIf(v > 50, 1, 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_partition_key_rewrites SELECT number, number % 100 FROM numbers(100);

SELECT
    'partition_key',
    countIf(explain LIKE '%Condition: (multiIf(greater(v, 50), 1, 0) in [1, 1])%'),
    countIf(explain LIKE '%Parts: 1/2%')
FROM (EXPLAIN indexes = 1 SELECT t FROM test_partition_key_rewrites WHERE multiIf(v > 50, 1, 0) = 1);

-- The key must not change the result.
SELECT 'partition_key_results';
SELECT count() FROM test_partition_key_rewrites WHERE multiIf(v > 50, 1, 0) = 1;
SELECT count() FROM test_partition_key_rewrites WHERE multiIf(v > 50, 1, 0) = 1 SETTINGS use_partition_pruning = 0;

DROP TABLE test_partition_key_rewrites;
