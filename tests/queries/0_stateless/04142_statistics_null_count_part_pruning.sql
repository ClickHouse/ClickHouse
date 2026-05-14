-- Test `NullCount` statistics: part pruning with `IS NULL` / `IS NOT NULL`.

SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_nullcount_pruning;

CREATE TABLE test_nullcount_pruning
(
    bucket UInt8,
    id UInt64,
    value Nullable(Int64) STATISTICS(nullcount),
    value_for_range Nullable(Int64) STATISTICS(minmax, nullcount),
    value_lc LowCardinality(Nullable(String)) STATISTICS(nullcount),
    value_lc_num LowCardinality(Nullable(Int64)) STATISTICS(minmax, nullcount)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Part 0: all NULL values.
INSERT INTO test_nullcount_pruning VALUES (0, 0, NULL, NULL, NULL, NULL), (0, 1, NULL, NULL, NULL, NULL);
-- Part 1: no NULL values, below the range predicate.
INSERT INTO test_nullcount_pruning VALUES (1, 2, 100, 100, 'a', 100), (1, 3, 101, 101, 'b', 101);
-- Part 2: no NULL values, inside the range predicate.
INSERT INTO test_nullcount_pruning VALUES (2, 4, 200, 200, 'c', 200), (2, 5, 201, 201, 'd', 201);
-- Part 3: mixed NULL and non-NULL values, inside the range predicate.
INSERT INTO test_nullcount_pruning VALUES (3, 6, NULL, NULL, NULL, NULL), (3, 7, 160, 160, 'e', 160), (3, 8, NULL, NULL, NULL, NULL), (3, 9, 161, 161, 'f', 161);

SELECT 'Test 1: `IS NULL` with `NullCount` prunes no-NULL parts';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL;

SELECT 'Test 2: `IS NOT NULL` with `NullCount` prunes all-NULL part';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL);
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL;

SELECT 'Test 3: `LowCardinality(Nullable)` `IS NULL` uses `NullCount`';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL);
SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL;

SELECT 'Test 4: `LowCardinality(Nullable)` `IS NOT NULL` uses `NullCount`';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NOT NULL);
SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NOT NULL;

SELECT 'Test 5: `MinMax` + `NullCount` prunes all-NULL and below-range parts';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000);
SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000;

SELECT 'Test 6: `NOT (value IS NULL)` matches `IS NOT NULL` pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE NOT (value IS NULL));
SELECT count() FROM test_nullcount_pruning WHERE NOT (value IS NULL);

SELECT 'Test 7: `IS NULL OR range` combines `NullCount` and `MinMax` pruning';
-- Part 0 matches via IS NULL; Parts 2 and 3 match via range >= 150; Part 1 is pruned.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150;

SELECT 'Test 8: `optimize_functions_to_subcolumns = 0` produces correct results without pruning';
-- Without the subcolumn rewrite the filter never mentions the virtual `.null` key, so this
-- path must not crash and must still return the right answer even if no parts are pruned.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 4/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'Test 9: `LowCardinality(Nullable(Int64))` with `MinMax` + `NullCount` prunes';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc_num >= 150 AND value_lc_num <= 5000);
SELECT count() FROM test_nullcount_pruning WHERE value_lc_num >= 150 AND value_lc_num <= 5000;

SELECT 'Test 10: `LowCardinality(Nullable)` `NOT (IS NULL)` works';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE NOT (value_lc_num IS NULL));
SELECT count() FROM test_nullcount_pruning WHERE NOT (value_lc_num IS NULL);

DROP TABLE test_nullcount_pruning;
