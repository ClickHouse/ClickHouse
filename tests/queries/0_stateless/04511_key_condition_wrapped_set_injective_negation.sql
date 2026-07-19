-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET optimize_rewrite_has_to_in = 0;

SET explain_query_plan_default = 'legacy';

-- { echo }

-- A wrapped-set atom built through an injective transform of a scalar predicate is exact
-- (`f(s) IN f(set)` is equivalent to `s IN set`), so negated membership can prune through it.
-- A transform that is not recognized as injective keeps the atom relaxed, and negation must not
-- prune through it. A relaxed atom would also disable pruning through the exact atoms of its
-- multi-atom group under `NOT`, so such atoms are dropped from the negated groups that have an
-- exact atom, and negation prunes through the exact atom alone.

-- `toString` declares itself injective.
DROP TABLE IF EXISTS test_injective;
CREATE TABLE test_injective (x UInt64) ENGINE = MergeTree
ORDER BY (toString(x), x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_injective SELECT intDiv(number, 4) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_injective WHERE NOT has([1], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_injective WHERE NOT has([1], x);
SELECT count() FROM test_injective WHERE NOT has([1], x) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- `NOT IN` is a complement-producing leaf (`notIn` is in `no_relaxed_atom_functions`), so the
-- wrapped pass does not run for it at all: only the direct atom is built.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_injective WHERE x NOT IN (1)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_injective WHERE x NOT IN (1);
SELECT count() FROM test_injective WHERE x NOT IN (1) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- Positive membership keeps pruning as before.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_injective WHERE has([1], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_injective WHERE has([1], x);
SELECT count() FROM test_injective WHERE has([1], x) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_injective;

-- `concat` with a constant suffix is mathematically injective, but it does not declare itself
-- injective, so its atom conservatively stays relaxed: it is dropped from the negated group,
-- and the negation prunes through the exact direct atom on `s` alone.
DROP TABLE IF EXISTS test_concat;
CREATE TABLE test_concat (s String) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_concat SELECT char(97 + intDiv(number, 4)) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_concat WHERE NOT has(['b'], s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_concat WHERE NOT has(['b'], s);
SELECT count() FROM test_concat WHERE NOT has(['b'], s) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_concat;

-- A genuinely non-injective transform: the wrapped atom stays relaxed, so it is dropped from
-- the negated group and the negation prunes through the exact direct atom on `s` alone.
DROP TABLE IF EXISTS test_noninjective;
CREATE TABLE test_noninjective (s String) ENGINE = MergeTree
ORDER BY (cityHash64(s) % 8, s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_noninjective SELECT char(97 + intDiv(number, 4)) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_noninjective WHERE NOT has(['b'], s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_noninjective WHERE NOT has(['b'], s);
SELECT count() FROM test_noninjective WHERE NOT has(['b'], s) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_noninjective;

-- An injective transform of one tuple component is still only a necessary condition of the
-- tuple membership: the component atom stays relaxed, so it is dropped from the negated group
-- and the negation prunes through the exact direct tuple atom alone, same as `NOT IN`.
DROP TABLE IF EXISTS test_tuple_negation;
CREATE TABLE test_tuple_negation (x UInt64, y UInt64) ENGINE = MergeTree
ORDER BY (toString(x), x, y)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_negation SELECT intDiv(number, 4), intDiv(number, 4) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_negation WHERE NOT has([(1, 1)], (x, y))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_negation WHERE NOT has([(1, 1)], (x, y));
SELECT count() FROM test_tuple_negation WHERE NOT has([(1, 1)], (x, y)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_negation WHERE (x, y) NOT IN ((1, 1))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_negation WHERE (x, y) NOT IN ((1, 1));
SELECT count() FROM test_tuple_negation WHERE (x, y) NOT IN ((1, 1)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_tuple_negation;

-- When the group has no exact atom, the relaxed atom is kept: it cannot prune under the
-- negation, and the query must still read everything and not lose rows.
DROP TABLE IF EXISTS test_only_relaxed;
CREATE TABLE test_only_relaxed (s String) ENGINE = MergeTree
ORDER BY concat(s, '_x')
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_only_relaxed SELECT char(97 + intDiv(number, 4)) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_only_relaxed WHERE NOT has(['b'], s)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_only_relaxed WHERE NOT has(['b'], s);
SELECT count() FROM test_only_relaxed WHERE NOT has(['b'], s) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_only_relaxed;
