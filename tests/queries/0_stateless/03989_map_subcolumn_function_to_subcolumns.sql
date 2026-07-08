-- Test: FunctionToSubcolumnsPass optimization of arrayElement on Map to key subcolumns.
-- Verifies that m['key'] is rewritten to m.key_<serialized_key> in EXPLAIN output.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_subcolumns;

-- ==========================================
-- Section 1: Basic optimization - Map(String, UInt64)
-- m['key'] in WHERE should be optimized to m.key_key subcolumn.
-- ==========================================

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map('key1', number, 'key2', number + 1) FROM numbers(10);

SELECT '-- Basic optimization: m[key1] in WHERE only';
-- After optimization, the EXPLAIN should show m.key_key1 instead of arrayElement(m, 'key1').
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
-- The original arrayElement should NOT appear.
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['key1'] > 5) WHERE explain LIKE '%arrayElement%';

-- ==========================================
-- Section 2: Optimization with full column also in SELECT
-- The optimization should still apply for Map because of transformers_always_optimize_with_full_column.
-- The key subcolumn is used in WHERE while the full map is in SELECT.
-- ==========================================

SELECT '-- Optimization when full column is also read in SELECT';
-- m.key_key1 should appear (for the WHERE clause).
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT m, id FROM t_map_subcolumns WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 3: Map column in ORDER BY (primary key) - should still optimize
-- because transformers_safe_with_indexes includes {Map, arrayElement}.
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY (id, m)
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map('key1', number, 'key2', number + 1) FROM numbers(10);

SELECT '-- Optimization when Map is in ORDER BY (primary key)';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 4: Non-constant key - should NOT optimize
-- arrayElement(m, col) where the key is not constant cannot be converted to a subcolumn.
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64), k String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map('key1', number), 'key1' FROM numbers(10);

SELECT '-- No optimization with non-constant key';
-- arrayElement should remain because the key is not a constant.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m[k] > 5) WHERE explain LIKE '%arrayElement%';

-- ==========================================
-- Section 5: Map(UInt64, String) - numeric key type
-- Verifies optimization works with non-string key types.
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map(1, 'a', 2, 'b') FROM numbers(10);

SELECT '-- Optimization with UInt64 key type';
-- The subcolumn name for numeric key 1 should be m.key_1.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m[1] = 'a') WHERE explain LIKE '%m.key_1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m[1] = 'a') WHERE explain LIKE '%arrayElement%';

-- ==========================================
-- Section 6: optimize_functions_to_subcolumns = 0 - optimization disabled
-- ==========================================

SELECT '-- No optimization when setting is disabled';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m[1] = 'a' SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%arrayElement%';

-- ==========================================
-- Section 7: Multiple key lookups on same map - all should optimize
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map('a', number, 'b', number + 1) FROM numbers(10);

SELECT '-- Multiple key lookups on same map both optimize';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['a'] > 3 AND m['b'] < 7) WHERE explain LIKE '%m.key_a%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['a'] > 3 AND m['b'] < 7) WHERE explain LIKE '%m.key_b%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['a'] > 3 AND m['b'] < 7) WHERE explain LIKE '%arrayElement%';

-- ==========================================
-- Section 8: basic serialization (not with_buckets)
-- The optimization still applies because DataTypeMap exposes dynamic subcolumns
-- regardless of serialization version. With basic serialization, the whole map is
-- read and the key is extracted; with_buckets reads only the relevant bucket.
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_map_subcolumns SELECT number, map('key1', number) FROM numbers(10);

SELECT '-- Optimization also works with basic (non-bucketed) serialization';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_subcolumns WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';

-- ==========================================
-- Section 9: Correctness - verify actual query results after optimization
-- ==========================================

DROP TABLE t_map_subcolumns;

CREATE TABLE t_map_subcolumns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t_map_subcolumns SELECT number, map('key1', number, 'key2', number * 10) FROM numbers(10);

SELECT '-- Correctness: results with optimization';
SELECT id, m['key1'], m['key2'] FROM t_map_subcolumns WHERE m['key1'] >= 7 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '-- Correctness: results without optimization match';
SELECT id, m['key1'], m['key2'] FROM t_map_subcolumns WHERE m['key1'] >= 7 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '-- Correctness: missing key returns default (0 for UInt64)';
SELECT id, m['nonexistent'] FROM t_map_subcolumns WHERE m['nonexistent'] = 0 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_subcolumns;
