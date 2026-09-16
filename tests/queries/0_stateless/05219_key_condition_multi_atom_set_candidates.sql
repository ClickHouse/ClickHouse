-- Pin the set mappings and granules selected by `IN` and `has`. Each pair occurs eight times
-- in two four-row granules. Standalone component and packed-tuple keys select six granules,
-- including the boundary granules.
-- The combined and derived keys expose extra pruning when multiple atoms are enabled.
-- Disable the `has` rewrite and count projections so both builders and physical reads are checked.
SET explain_query_plan_default = 'legacy';
SET use_query_condition_cache = 0;
SET optimize_use_projections = 0;
SET optimize_trivial_count_query = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;
SET optimize_move_to_prewhere = 0;
SET max_threads = 1;
SET use_primary_key = 1;
SET use_skip_indexes = 0;
SET optimize_rewrite_has_to_in = 0;

DROP TABLE IF EXISTS test_set_candidates_components;
CREATE TABLE test_set_candidates_components (s String, x UInt8, id UInt32) ENGINE = MergeTree
ORDER BY (s, x)
SETTINGS index_granularity = 4, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_set_candidates_components
SELECT char(97 + intDiv(number, 64)), toUInt8(intDiv(number % 64, 8)), number FROM numbers(256);

SELECT 'components, multiple_atoms=0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_components WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_components WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 24;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_components WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_components WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 24;

SELECT 'components, multiple_atoms=1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_components WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_components WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 24;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_components WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_components WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 24;

DROP TABLE test_set_candidates_components;

DROP TABLE IF EXISTS test_set_candidates_packed;
CREATE TABLE test_set_candidates_packed (s String, x UInt8, id UInt32) ENGINE = MergeTree
ORDER BY (tuple(s, x), id)
SETTINGS index_granularity = 4, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_set_candidates_packed
SELECT char(97 + intDiv(number, 64)), toUInt8(intDiv(number % 64, 8)), number FROM numbers(256);

SELECT 'packed, multiple_atoms=0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_packed WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_packed WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 24;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_packed WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_packed WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 24;

SELECT 'packed, multiple_atoms=1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_packed WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_packed WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 24;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_packed WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_packed WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 24;

DROP TABLE test_set_candidates_packed;

DROP TABLE IF EXISTS test_set_candidates_combined;
CREATE TABLE test_set_candidates_combined (s String, x UInt8, id UInt32) ENGINE = MergeTree
ORDER BY (tuple(s, x), s, x, cityHash64(s), x + 1)
SETTINGS index_granularity = 4, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_set_candidates_combined
SELECT char(97 + intDiv(number, 64)), toUInt8(intDiv(number % 64, 8)), number FROM numbers(256);

SELECT 'combined, multiple_atoms=0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 136;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 136;

-- Negation keeps the exact tuple mappings. It excludes two fully covered granules.
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE (s, x) NOT IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE (s, x) NOT IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 248;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE NOT has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE NOT has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 248;

SELECT 'combined, multiple_atoms=1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 24;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 24;

-- Negation keeps the exact tuple mappings. It excludes two fully covered granules.
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE (s, x) NOT IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE (s, x) NOT IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 248;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_combined WHERE NOT has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_combined WHERE NOT has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 248;

DROP TABLE test_set_candidates_combined;

DROP TABLE IF EXISTS test_set_candidates_derived;
CREATE TABLE test_set_candidates_derived (s String, x UInt8, id UInt32) ENGINE = MergeTree
ORDER BY (cityHash64(s), x + 1, s, x)
SETTINGS index_granularity = 4, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_set_candidates_derived
SELECT char(97 + intDiv(number, 64)), toUInt8(intDiv(number % 64, 8)), number FROM numbers(256);

SELECT 'derived, multiple_atoms=0';
SET analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_derived WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_derived WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 136;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_derived WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_derived WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 136;

SELECT 'derived, multiple_atoms=1';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_derived WHERE (s, x) IN (('b', 2), ('d', 5))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_derived WHERE (s, x) IN (('b', 2), ('d', 5)) SETTINGS max_rows_to_read = 40;
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM test_set_candidates_derived WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x))
)
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_set_candidates_derived WHERE has([('b', toUInt8(2)), ('d', toUInt8(5))], (s, x)) SETTINGS max_rows_to_read = 40;

DROP TABLE test_set_candidates_derived;
