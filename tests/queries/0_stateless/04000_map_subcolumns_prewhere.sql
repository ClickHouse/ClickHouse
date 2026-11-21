-- Test: Map subcolumns in PREWHERE when the whole map is also read.
-- Verifies via EXPLAIN that FunctionToSubcolumnsPass rewrites m['key'] to m.key_<key>
-- in PREWHERE, while the full map is still read for SELECT. Also checks correctness.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

-- ==========================================
-- Section 1: PREWHERE optimization with with_buckets (wide parts)
-- m['key'] in PREWHERE should become m.key_<key> subcolumn.
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(String, UInt64), v UInt64)
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

INSERT INTO t_map_prewhere SELECT number, map('key1', number, 'key2', number * 10), number % 5 FROM numbers(100);

-- EXPLAIN should show m.key_key1 in Prewhere filter (not arrayElement)
SELECT '-- Section 1: PREWHERE optimization with with_buckets';
SELECT '-- PREWHERE m[key1] is rewritten to m.key_key1';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['key1'] > 90
) WHERE explain LIKE '%m.key_key1%';

SELECT '-- arrayElement should NOT appear in PREWHERE';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['key1'] > 90
) WHERE explain LIKE '%arrayElement%';

-- Correctness: results should be correct
SELECT '-- Correctness: PREWHERE m[key1] > 90';
SELECT id, m, m['key1'], m['key2'] FROM t_map_prewhere PREWHERE m['key1'] > 90 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

-- Same query without optimization should produce same results
SELECT '-- Correctness: same without optimization';
SELECT id, m, m['key1'], m['key2'] FROM t_map_prewhere PREWHERE m['key1'] > 90 ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_prewhere;

-- ==========================================
-- Section 2: PREWHERE with WHERE combined
-- PREWHERE filters on map subcolumn, WHERE filters on another column.
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(String, UInt64), v UInt64)
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

INSERT INTO t_map_prewhere SELECT number, map('a', number, 'b', number * 2), number % 7 FROM numbers(100);

SELECT '-- Section 2: PREWHERE + WHERE combined';
SELECT '-- PREWHERE on map subcolumn, WHERE on v';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['a'] > 80 WHERE v < 3
) WHERE explain LIKE '%m.key_a%';

SELECT '-- Correctness: PREWHERE m[a] > 80 WHERE v < 3';
SELECT id, m['a'], m['b'], v FROM t_map_prewhere PREWHERE m['a'] > 80 WHERE v < 3 ORDER BY id;

DROP TABLE t_map_prewhere;

-- ==========================================
-- Section 3: PREWHERE with multiple map key lookups
-- Both m['key1'] and m['key2'] in PREWHERE should be optimized.
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(String, UInt64))
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

INSERT INTO t_map_prewhere SELECT number, map('x', number, 'y', 100 - number) FROM numbers(100);

SELECT '-- Section 3: Multiple map key lookups in PREWHERE';
SELECT '-- Both m.key_x and m.key_y should appear';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['x'] > 70 AND m['y'] > 70
) WHERE explain LIKE '%m.key_x%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['x'] > 70 AND m['y'] > 70
) WHERE explain LIKE '%m.key_y%';

SELECT '-- arrayElement should NOT appear';
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['x'] > 70 AND m['y'] > 70
) WHERE explain LIKE '%arrayElement%';

-- The condition m['x'] > 70 AND m['y'] > 70 means x > 70 AND (100-x) > 70, i.e. x in (71..29) — empty
SELECT '-- Correctness: PREWHERE m[x] > 70 AND m[y] > 70 (empty result expected)';
SELECT count() FROM t_map_prewhere PREWHERE m['x'] > 70 AND m['y'] > 70;

-- Non-empty range: m['x'] > 90 AND m['y'] < 20 means x > 90 AND (100-x) < 20, i.e. x > 90 AND x > 80 → x > 90
SELECT '-- Correctness: PREWHERE m[x] > 90 AND m[y] < 20';
SELECT id, m['x'], m['y'] FROM t_map_prewhere PREWHERE m['x'] > 90 AND m['y'] < 20 ORDER BY id;

DROP TABLE t_map_prewhere;

-- ==========================================
-- Section 4: PREWHERE with basic serialization (non-bucketed)
-- Optimization still applies, but reads the whole map internally.
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1;

INSERT INTO t_map_prewhere SELECT number, map('key1', number, 'key2', number * 10) FROM numbers(100);

SELECT '-- Section 4: PREWHERE with basic serialization';
SELECT '-- Optimization still rewrites to subcolumn';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['key1'] > 95
) WHERE explain LIKE '%m.key_key1%';

SELECT '-- Correctness: PREWHERE with basic serialization';
SELECT id, m['key1'], m['key2'] FROM t_map_prewhere PREWHERE m['key1'] > 95 ORDER BY id;

DROP TABLE t_map_prewhere;

-- ==========================================
-- Section 5: PREWHERE on map subcolumn with UInt64 key type
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(UInt64, String))
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

INSERT INTO t_map_prewhere SELECT number, map(1, toString(number), 2, toString(number * 10)) FROM numbers(100);

SELECT '-- Section 5: PREWHERE with UInt64 key';
SELECT '-- m.key_1 should appear';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m[1] = '99'
) WHERE explain LIKE '%m.key_1%';

SELECT '-- Correctness: PREWHERE m[1] = 99';
SELECT id, m[1], m[2] FROM t_map_prewhere PREWHERE m[1] = '99' ORDER BY id;

DROP TABLE t_map_prewhere;

-- ==========================================
-- Section 6: PREWHERE disabled optimization vs enabled
-- When optimize_functions_to_subcolumns = 0, arrayElement should remain.
-- ==========================================

DROP TABLE IF EXISTS t_map_prewhere;
CREATE TABLE t_map_prewhere (id UInt64, m Map(String, UInt64))
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

INSERT INTO t_map_prewhere SELECT number, map('k', number) FROM numbers(100);

SELECT '-- Section 6: Optimization disabled';
SELECT '-- arrayElement should remain when optimization disabled';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, m FROM t_map_prewhere PREWHERE m['k'] > 90
    SETTINGS optimize_functions_to_subcolumns = 0
) WHERE explain LIKE '%arrayElement%';

SELECT '-- Both produce same result';
SELECT count() FROM t_map_prewhere PREWHERE m['k'] > 90
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT count() FROM t_map_prewhere PREWHERE m['k'] > 90
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_prewhere;
