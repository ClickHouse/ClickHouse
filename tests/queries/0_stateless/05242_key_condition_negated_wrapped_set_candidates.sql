-- Tags: no-random-settings, no-random-merge-tree-settings
-- Selected marks depend on fixed granularity and index-analysis settings.
-- Each table holds 48 excluded rows and 16 matching rows in one-row granules. The 17-row read limit
-- allows the matching rows and one boundary granule, so it requires pruning through an exact atom.

SET optimize_rewrite_has_to_in = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_use_projections = 0;
SET optimize_move_to_prewhere = 0;
SET use_statistics_for_part_pruning = 0;
SET use_query_condition_cache = 0;

-- An earlier non-injective key must not hide a later exact transform of the same scalar.
DROP TABLE IF EXISTS test_negated_wrapped_sets;
CREATE TABLE test_negated_wrapped_sets (s String) ENGINE = MergeTree ORDER BY (cityHash64(s), reverse(s))
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_negated_wrapped_sets SELECT if(number < 48, 'abc', 'xyz') FROM numbers(64);

SELECT 'scalar, multiple atoms = 0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE s NOT IN ('abc'));
SELECT count() FROM test_negated_wrapped_sets WHERE s NOT IN ('abc')
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas(['abc'], s)
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s)
SETTINGS max_rows_to_read = 64;

SELECT 'scalar, multiple atoms = 1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE s NOT IN ('abc'));
SELECT count() FROM test_negated_wrapped_sets WHERE s NOT IN ('abc')
SETTINGS max_rows_to_read = 17;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas(['abc'], s)
SETTINGS max_rows_to_read = 17;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s)
SETTINGS max_rows_to_read = 17;

DROP TABLE test_negated_wrapped_sets;

-- Reversing the whole tuple preserves every component, so its negated set atom can be exact.
DROP TABLE IF EXISTS test_negated_wrapped_sets;
CREATE TABLE test_negated_wrapped_sets (s String, x UInt8) ENGINE = MergeTree ORDER BY (cityHash64(tuple(s, x)), reverse(tuple(s, x)))
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_negated_wrapped_sets SELECT if(number < 48, 'abc', 'xyz'), if(number < 48, 1, 2) FROM numbers(64);

SELECT 'whole tuple, multiple atoms = 0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1)));
SELECT count() FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1))
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 64;

SELECT 'whole tuple, multiple atoms = 1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1)));
SELECT count() FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1))
SETTINGS max_rows_to_read = 17;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 17;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 17;

DROP TABLE test_negated_wrapped_sets;

-- Reversing only one component loses the other component. Negating that projected set would
-- discard the matching rows whose string agrees with the excluded tuple but whose number differs.
DROP TABLE IF EXISTS test_negated_wrapped_sets;
CREATE TABLE test_negated_wrapped_sets (s String, x UInt8) ENGINE = MergeTree ORDER BY (cityHash64(tuple(s, x)), cityHash64(s), reverse(s))
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_negated_wrapped_sets SELECT 'abc', if(number < 48, 1, 2) FROM numbers(64);

SELECT 'tuple component, multiple atoms = 1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1)));
SELECT count() FROM test_negated_wrapped_sets WHERE (s, x) NOT IN (('abc', 1))
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x)));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has([('abc', 1)], (s, x))
SETTINGS max_rows_to_read = 64;

DROP TABLE test_negated_wrapped_sets;

-- A later non-injective transform cannot supply an exact complement: both strings have length 3.
DROP TABLE IF EXISTS test_negated_wrapped_sets;
CREATE TABLE test_negated_wrapped_sets (s String) ENGINE = MergeTree ORDER BY (cityHash64(s), length(s))
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_negated_wrapped_sets SELECT if(number < 48, 'abc', 'xyz') FROM numbers(64);

SELECT 'non-injective, multiple atoms = 1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE s NOT IN ('abc'));
SELECT count() FROM test_negated_wrapped_sets WHERE s NOT IN ('abc')
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE notHas(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE notHas(['abc'], s)
SETTINGS max_rows_to_read = 64;
SELECT marks FROM (EXPLAIN ESTIMATE SELECT * FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s));
SELECT count() FROM test_negated_wrapped_sets WHERE NOT has(['abc'], s)
SETTINGS max_rows_to_read = 64;

DROP TABLE test_negated_wrapped_sets;
