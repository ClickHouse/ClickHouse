-- Test NULL count from `Basic` statistics: part pruning with `IS NULL` / `IS NOT NULL`
-- and tightened min/max ranges on Nullable columns.

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
    value Nullable(Int64) STATISTICS(basic),
    value_for_range Nullable(Int64) STATISTICS(basic),
    value_lc LowCardinality(Nullable(String)) STATISTICS(basic),
    value_lc_num LowCardinality(Nullable(Int64)) STATISTICS(basic)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY id
-- `nullable_serialization_version` is pinned: randomized `allow_sparse` enables the
-- sparsity-filter trivial count rewrite for `count() ... WHERE col IS NULL`, which
-- replaces ReadFromMergeTree in EXPLAIN and hides the `Statistics` index section.
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Part 0: all NULL values.
INSERT INTO test_nullcount_pruning VALUES (0, 0, NULL, NULL, NULL, NULL), (0, 1, NULL, NULL, NULL, NULL);
-- Part 1: no NULL values, below the range predicate.
INSERT INTO test_nullcount_pruning VALUES (1, 2, 100, 100, 'a', 100), (1, 3, 101, 101, 'b', 101);
-- Part 2: no NULL values, inside the range predicate.
INSERT INTO test_nullcount_pruning VALUES (2, 4, 200, 200, 'c', 200), (2, 5, 201, 201, 'd', 201);
-- Part 3: mixed NULL and non-NULL values, inside the range predicate.
INSERT INTO test_nullcount_pruning VALUES (3, 6, NULL, NULL, NULL, NULL), (3, 7, 160, 160, 'e', 160), (3, 8, NULL, NULL, NULL, NULL), (3, 9, 161, 161, 'f', 161);

SELECT 'Test 1: `IS NULL` prunes no-NULL parts via NULL count';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL;

SELECT 'Test 2: `IS NOT NULL` prunes all-NULL part via NULL count';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL);
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL;

SELECT 'Test 3: `LowCardinality(Nullable)` `IS NULL` pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL);
SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL;

SELECT 'Test 4: `LowCardinality(Nullable)` `IS NOT NULL` pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NOT NULL);
SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NOT NULL;

SELECT 'Test 5: min/max + NULL count prune all-NULL and below-range parts';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000);
SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000;

SELECT 'Test 6: `NOT (value IS NULL)` matches `IS NOT NULL` pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE NOT (value IS NULL));
SELECT count() FROM test_nullcount_pruning WHERE NOT (value IS NULL);

SELECT 'Test 7: `IS NULL OR range` combines NULL count and min/max pruning';
-- Part 0 matches via IS NULL; parts 2 and 3 match via range >= 150; part 1 is pruned.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150;

SELECT 'Test 8: `optimize_functions_to_subcolumns = 0` prunes via native `IS NULL` atoms';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'Test 8b: `optimize_functions_to_subcolumns = 0` range pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000 SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value_for_range >= 150 AND value_for_range <= 5000 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'Test 8c: `optimize_functions_to_subcolumns = 0` `IS NULL OR range` combination';
-- Native IS NULL atom keeps part 0, range atom keeps parts 2 and 3; part 1 is pruned.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150 SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL OR value_for_range >= 150 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'Test 9: `LowCardinality(Nullable(Int64))` range pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc_num >= 150 AND value_lc_num <= 5000);
SELECT count() FROM test_nullcount_pruning WHERE value_lc_num >= 150 AND value_lc_num <= 5000;

SELECT 'Test 10: `LowCardinality(Nullable)` `NOT (IS NULL)` works';
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 3/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE NOT (value_lc_num IS NULL));
SELECT count() FROM test_nullcount_pruning WHERE NOT (value_lc_num IS NULL);

SELECT 'Test 11: tightened range (null_count = 0) prunes parts below the predicate';
-- null_count = 0 tightens the range to [min, max]; a strict > range also excludes the NULL sentinel, so only part 3 survives.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 1/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value > 250);
SELECT count() FROM test_nullcount_pruning WHERE value > 250;

SELECT 'Test 12: upper-bound predicate prunes the all-NULL part';
-- The all-NULL part has the sentinel range [+inf, +inf], which does not intersect (-inf, 100).
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 0/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value < 100);
SELECT count() FROM test_nullcount_pruning WHERE value < 100;

SELECT 'Test 13: `optimize_functions_to_subcolumns = 0` `IS NULL` on `LowCardinality(Nullable(String))`';
-- No min/max for String: null_count = 0 yields the open range (-inf, +inf), which the native IS NULL atom contains.
SELECT countIf(explain LIKE '%Statistics%') > 0, countIf(explain LIKE '%Parts: 2/4%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() FROM test_nullcount_pruning WHERE value_lc IS NULL SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'Test 15: `use_statistics_for_part_pruning = 0` disables pruning';
SELECT countIf(explain LIKE '%Statistics%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL SETTINGS use_statistics_for_part_pruning = 0);

DROP TABLE test_nullcount_pruning;

DROP TABLE IF EXISTS test_float_inf_pruning;
CREATE TABLE test_float_inf_pruning
(
    bucket UInt8,
    f Nullable(Float64) STATISTICS(basic)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY bucket
-- Pinned for the same reason as above: keep `count() ... WHERE f IS NULL` on the
-- ReadFromMergeTree path so that the `Statistics` index section stays in EXPLAIN.
SETTINGS auto_statistics_types = '', nullable_serialization_version = 'basic';

-- Part 0: no NULLs, huge finite value and a real +Inf (NOT the +infinity NULL sentinel).
INSERT INTO test_float_inf_pruning VALUES (0, 1e308), (0, inf);
-- Part 1: all NULL.
INSERT INTO test_float_inf_pruning VALUES (1, NULL), (1, NULL);

SELECT 'Test 14a: real +Inf max does not disable tightened-range pruning';
SELECT countIf(explain LIKE '%Parts: 0/2%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_float_inf_pruning WHERE f < 1e300);
SELECT count() FROM test_float_inf_pruning WHERE f < 1e300;

SELECT 'Test 14b: `IS NULL` with real +Inf max';
-- A real +Inf value must not be confused with the +infinity NULL sentinel.
SELECT countIf(explain LIKE '%Parts: 1/2%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_float_inf_pruning WHERE f IS NULL);
SELECT count() FROM test_float_inf_pruning WHERE f IS NULL;

DROP TABLE test_float_inf_pruning;

DROP TABLE IF EXISTS test_dot_null_column;
CREATE TABLE test_dot_null_column
(
    bucket UInt8,
    `foo.null` Nullable(Int64) STATISTICS(basic)
)
ENGINE = MergeTree()
PARTITION BY bucket
ORDER BY bucket
SETTINGS auto_statistics_types = '';

INSERT INTO test_dot_null_column VALUES (0, NULL), (0, 5);
INSERT INTO test_dot_null_column VALUES (1, 10), (1, 20);

SELECT 'Test 16: user column literally named `foo.null` is not treated as a virtual key';
SELECT count() FROM test_dot_null_column WHERE `foo.null` IS NULL;
SELECT count() FROM test_dot_null_column WHERE `foo.null` > 8;

DROP TABLE test_dot_null_column;
