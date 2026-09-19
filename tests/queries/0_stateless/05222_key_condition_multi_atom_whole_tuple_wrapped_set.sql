-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: granule counts may differ with random settings.

-- A key column can be a deterministic function of the packed tuple of a membership predicate,
-- such as `cityHash64(tuple(s, x))` for `(s, x) IN (...)`. It is not a function of any single
-- tuple component, so the wrapped-set pass must also consider the whole tuple as a source.
-- Without that atom the leaf constrains only the second key column, which cannot prune any
-- granule whose first key column spans several values.

SET optimize_rewrite_has_to_in = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE IF EXISTS test_whole_tuple_wrapped_set;

CREATE TABLE test_whole_tuple_wrapped_set (s String, x UInt8) ENGINE = MergeTree
ORDER BY (cityHash64(tuple(s, x)), tuple(s, x))
SETTINGS index_granularity = 8, add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_whole_tuple_wrapped_set SELECT toString(number), number FROM numbers(200);

-- The direct packed-tuple atom and the wrapped `cityHash64(tuple(s, x))` atom prune together.
SELECT count() FROM test_whole_tuple_wrapped_set WHERE (s, x) IN (('7', 7)) SETTINGS force_primary_key = 1, max_rows_to_read = 16;
SELECT count() FROM test_whole_tuple_wrapped_set WHERE (s, x) IN (('7', 7), ('120', 120)) SETTINGS force_primary_key = 1, max_rows_to_read = 32;
SELECT count() FROM test_whole_tuple_wrapped_set WHERE has([('7', 7)], (s, x)) SETTINGS force_primary_key = 1, max_rows_to_read = 16;
SELECT count() FROM test_whole_tuple_wrapped_set WHERE has([('7', 7), ('120', 120)], (s, x)) SETTINGS force_primary_key = 1, max_rows_to_read = 32;

-- The wrapped atom is relaxed (the hash is not injective), so negation stays correct.
SELECT count() FROM test_whole_tuple_wrapped_set WHERE (s, x) NOT IN (('7', 7)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_whole_tuple_wrapped_set WHERE NOT has([('7', 7)], (s, x)) SETTINGS force_primary_key = 1;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_whole_tuple_wrapped_set WHERE (s, x) IN (('7', 7)))
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_whole_tuple_wrapped_set WHERE has([('7', 7)], (s, x)))
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';

-- With the multi-atom analysis disabled the leaf keeps only its direct atom and reads every granule.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_whole_tuple_wrapped_set WHERE (s, x) IN (('7', 7))
    SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_whole_tuple_wrapped_set;
