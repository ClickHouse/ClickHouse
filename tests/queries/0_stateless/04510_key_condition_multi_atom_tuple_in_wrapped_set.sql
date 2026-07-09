SET explain_query_plan_default = 'legacy';

-- { echo }

-- A tuple membership predicate builds wrapped-set atoms per tuple component: a key column that
-- is a deterministic function of one component is constrained by transforming that component of
-- the set elements, in addition to the direct atom over the components themselves.

DROP TABLE IF EXISTS test_tuple_in;
CREATE TABLE test_tuple_in (s String, x UInt64) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), s, x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_in SELECT char(97 + intDiv(number, 4)), number FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- A multi-element set spanning two values of the leading column.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5), ('d', 14))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5), ('d', 14));
SELECT count() FROM test_tuple_in WHERE (s, x) IN (('b', 5), ('d', 14)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- NOT IN must not build the relaxed wrapped atoms and must not prune wrongly.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_in WHERE (s, x) NOT IN (('b', 5))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_in WHERE (s, x) NOT IN (('b', 5));
SELECT count() FROM test_tuple_in WHERE (s, x) NOT IN (('b', 5)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_tuple_in;

-- Both components have derived key columns: each contributes its own wrapped atom.
DROP TABLE IF EXISTS test_tuple_in_two;
CREATE TABLE test_tuple_in_two (s String, x UInt64) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), x * 2, s, x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_in_two SELECT char(97 + intDiv(number, 4)), number FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_in_two WHERE (s, x) IN (('b', 5))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_in_two WHERE (s, x) IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_in_two WHERE (s, x) IN (('b', 5)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_tuple_in_two;

-- The same through `has` with an array of tuples.
DROP TABLE IF EXISTS test_tuple_has;
CREATE TABLE test_tuple_has (s String, x UInt64) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), s, x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_has SELECT char(97 + intDiv(number, 4)), number FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_tuple_has WHERE has([('b', 5), ('d', 14)], (s, x))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_tuple_has WHERE has([('b', 5), ('d', 14)], (s, x));
SELECT count() FROM test_tuple_has WHERE has([('b', 5), ('d', 14)], (s, x)) SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_tuple_has;
