-- A 16-leaf `OR` has 31 template elements; a 17-leaf `OR` has 33 and exceeds the 32-element limit.
-- Each leaf expands into two index atoms. The combined skip-index result must retain the original
-- template positions: individual indexes cannot prune the disjunction on their own.
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;
SET max_threads = 1;
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SET use_skip_indexes = 1;
SET use_primary_key = 1;
SET use_skip_indexes_on_data_read = 0;
SET optimize_min_equality_disjunction_chain_length = 1000;

DROP TABLE IF EXISTS test_multi_atom_disjunction_template;
CREATE TABLE test_multi_atom_disjunction_template
(
    id UInt32,
    a UInt32,
    b UInt32,
    INDEX ia (intDiv(a, 8), a) TYPE minmax GRANULARITY 1,
    INDEX ib (intDiv(b, 8), b) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_multi_atom_disjunction_template SELECT number, number, number FROM numbers(128);

SELECT '16 leaves, disjunction pruning=0';
SET use_skip_indexes_for_disjunctions = 0;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
SETTINGS max_rows_to_read = 128;

SELECT '16 leaves, disjunction pruning=1';
SET use_skip_indexes_for_disjunctions = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
SETTINGS max_rows_to_read = 52;

SELECT '17 leaves, disjunction pruning=1';
SET use_skip_indexes_for_disjunctions = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
    OR a = 51
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_disjunction_template
WHERE a = 3
    OR b = 6
    OR a = 9
    OR b = 12
    OR a = 15
    OR b = 18
    OR a = 21
    OR b = 24
    OR a = 27
    OR b = 30
    OR a = 33
    OR b = 36
    OR a = 39
    OR b = 42
    OR a = 45
    OR b = 48
    OR a = 51
SETTINGS max_rows_to_read = 128;

DROP TABLE test_multi_atom_disjunction_template;
