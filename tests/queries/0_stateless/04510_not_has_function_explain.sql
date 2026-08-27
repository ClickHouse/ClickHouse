-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET explain_query_plan_default = 'legacy';

-- { echo }

DROP TABLE IF EXISTS test_not_has;
CREATE TABLE test_not_has (x UInt64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has SELECT intDiv(number, 4) FROM numbers(24);

-- Without the analyzer rewrite, `notHas` prunes through its own key condition atom, which is the
-- complement of the `has` atom.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- `NOT has` is pushed down to the `notHas` leaf during index analysis, and `NOT notHas` folds back
-- to `has`, so both prune without the analyzer rewrite.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE NOT has([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE NOT has([1], x) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE NOT notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE NOT notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- When the analyzer rewrite is blocked (NULL array element), the key condition atom still applies:
-- a NULL element can never match the non-Nullable key column, so it is dropped from the set.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE notHas([1, NULL], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x);
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has WHERE NOT has([1, NULL], x);
SELECT count() FROM test_not_has WHERE NOT has([1, NULL], x) SETTINGS use_primary_key = 0;

DROP TABLE test_not_has;

-- The same NULL-element rule applies to `NOT IN` when a subquery set contains a NULL and
-- `transform_null_in` is enabled: the NULL must be dropped from the set, not decayed to the
-- nested type's default value, which would poison the set and prune granules with matching rows.
DROP TABLE IF EXISTS test_null_set;
CREATE TABLE test_null_set (x UInt64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_null_set SELECT intDiv(number, 4) FROM numbers(24);

SET transform_null_in = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL]))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL]));
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin([1, NULL])) SETTINGS use_primary_key = 0;

-- The positive form keeps only the matching rows.
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin([1, NULL]));
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin([1, NULL])) SETTINGS use_primary_key = 0;

-- A set of only NULLs becomes empty: nothing matches the positive form, everything matches the
-- negated form. The set must be typed: an untyped `[NULL]` produces a set of `Nothing`, which the
-- `IN` execution itself cannot compare against the column.
SELECT count() FROM test_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));
SELECT count() FROM test_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));

DROP TABLE test_null_set;

-- A NULL set element must be preserved for a Nullable key. It matches the NULL key row, so the
-- negated form can skip that one-row granule.
DROP TABLE IF EXISTS test_nullable_null_set;
CREATE TABLE test_nullable_null_set (x Nullable(UInt64)) ENGINE = MergeTree
ORDER BY x
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_nullable_null_set VALUES (NULL), (1), (2);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x);
SELECT count() FROM test_nullable_null_set WHERE notHas([NULL], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_nullable_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')));
SELECT count() FROM test_nullable_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_primary_key = 0;

DROP TABLE test_nullable_null_set;
