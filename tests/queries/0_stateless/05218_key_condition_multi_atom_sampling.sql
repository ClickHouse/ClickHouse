-- Sampling appends bounds to the predicate's key condition. Pin those bounds and the selected
-- granules in both primary-index representations and sort directions. Reading limits ensure
-- that pruning is applied during execution, with count projections disabled.
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;
SET max_threads = 1;
SET use_primary_key = 1;
SET use_skip_indexes = 0;

DROP TABLE IF EXISTS test_multi_atom_sampling_asc;
CREATE TABLE test_multi_atom_sampling_asc (x UInt8) ENGINE = MergeTree
ORDER BY (intDiv(x, 16), x) SAMPLE BY x
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0, allow_experimental_reverse_key = 1;
INSERT INTO test_multi_atom_sampling_asc SELECT number FROM numbers(256);

SET use_lightweight_primary_key_index_analysis = 0;
SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'asc, sparse=0, multiple_atoms=0, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 192;

SELECT 'asc, sparse=0, multiple_atoms=0, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 168;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'asc, sparse=0, multiple_atoms=1, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 160;

SELECT 'asc, sparse=0, multiple_atoms=1, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 136;

SET use_lightweight_primary_key_index_analysis = 1;
SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'asc, sparse=1, multiple_atoms=0, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 192;

SELECT 'asc, sparse=1, multiple_atoms=0, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 168;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'asc, sparse=1, multiple_atoms=1, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 160;

SELECT 'asc, sparse=1, multiple_atoms=1, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_asc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 136;

DROP TABLE test_multi_atom_sampling_asc;

DROP TABLE IF EXISTS test_multi_atom_sampling_desc;
CREATE TABLE test_multi_atom_sampling_desc (x UInt8) ENGINE = MergeTree
ORDER BY (intDiv(x, 16) DESC, x DESC) SAMPLE BY x
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0, allow_experimental_reverse_key = 1;
INSERT INTO test_multi_atom_sampling_desc SELECT number FROM numbers(256);

SET use_lightweight_primary_key_index_analysis = 0;
SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'desc, sparse=0, multiple_atoms=0, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 192;

SELECT 'desc, sparse=0, multiple_atoms=0, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 168;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'desc, sparse=0, multiple_atoms=1, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 160;

SELECT 'desc, sparse=0, multiple_atoms=1, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 136;

SET use_lightweight_primary_key_index_analysis = 1;
SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'desc, sparse=1, multiple_atoms=0, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 192;

SELECT 'desc, sparse=1, multiple_atoms=0, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 168;
SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'desc, sparse=1, multiple_atoms=1, SAMPLE 1/2 OFFSET 1/4';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 1/2 OFFSET 1/4 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 160;

SELECT 'desc, sparse=1, multiple_atoms=1, SAMPLE 3/10 OFFSET 1/5';
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_multi_atom_sampling_desc SAMPLE 3/10 OFFSET 1/5 WHERE x >= 32 AND x < 224 SETTINGS max_rows_to_read = 136;

DROP TABLE test_multi_atom_sampling_desc;
