-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET rewrite_in_to_join = 0;

DROP TABLE IF EXISTS ref_colors;
CREATE TABLE ref_colors
(
    id UInt64,
    name String,
    n UInt64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO ref_colors VALUES
    (1, 'red',   5),
    (2, 'blue',  7),
    (3, 'red',  12),
    (4, 'green', 0),
    (5, 'Rose',  9);

DROP DICTIONARY IF EXISTS colors;
CREATE DICTIONARY colors
(
  id   UInt64,
  name String,
  n    UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_colors'))
LAYOUT(HASHED())
LIFETIME(0);

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    color_id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY color_id;

INSERT INTO t VALUES
    (1, 'a'),
    (2, 'b'),
    (3, 'c'),
    (4, 'd'),
    (5, 'R');

-- Add a key that doesn't exist in the dictionary to test missing key behavior
INSERT INTO t VALUES (99, 'missing_key');

-- Test IN with default value in list - should produce NOT(key IN (SELECT ... WHERE attr NOT IN S))
-- Default value for String is '', so '' IN ('') should match missing keys
SELECT 'IN with default value - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('')
ORDER BY color_id, payload;

-- Missing key 99 should match because '' (default) IN ('') = true
SELECT 'IN with default value - result';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('')
ORDER BY color_id, payload;

-- Test IN with default value in list (multiple values including default)
SELECT 'IN with default value among others - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', '')
ORDER BY color_id, payload;

SELECT 'IN with default value among others - result';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', '')
ORDER BY color_id, payload;

-- Test notIn with default value in list - should NOT have OR clause
-- Missing key returns '', '' NOT IN ('') = false, so missing keys should not match
SELECT 'notIn with default value - plan (should NOT have OR clause)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('')
ORDER BY color_id, payload;

-- Missing key 99 should NOT match because '' NOT IN ('') = false
SELECT 'notIn with default value - result';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('')
ORDER BY color_id, payload;

-- Test notIn with default value NOT in list - should produce NOT(key IN (SELECT ... WHERE attr IN S))
-- Missing key returns '', '' NOT IN ('red', 'blue') = true, so missing keys should match
SELECT 'notIn without default value - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

-- Missing key 99 should match because '' NOT IN ('red', 'blue') = true
SELECT 'notIn without default value - result';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

-- Test IN with numeric attribute where default (0) is in the list
-- Default for UInt64 is 0, so this tests numeric default handling
SELECT 'IN with numeric default - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) IN (0)
ORDER BY color_id, payload;

-- Missing key 99 should match because 0 (default) IN (0) = true
-- Also key 4 matches because its n value is 0
SELECT 'IN with numeric default - result';
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) IN (0)
ORDER BY color_id, payload;

-- Clean up the extra row before other tests
DELETE FROM t WHERE color_id = 99;

-- Test IN with constant tuple
SELECT 'IN with constant tuple - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'IN with constant tuple';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

-- Test notIn with constant tuple
SELECT 'notIn with constant tuple - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'notIn with constant tuple';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

-- Test IN with numeric attribute
SELECT 'IN with numeric attribute - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) IN (5, 7)
ORDER BY color_id, payload;

SELECT 'IN with numeric attribute';
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) IN (5, 7)
ORDER BY color_id, payload;

-- Test IN with single value
SELECT 'IN with single value - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red')
ORDER BY color_id, payload;

SELECT 'IN with single value';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red')
ORDER BY color_id, payload;

-- Test IN with Array form (should work the same as Tuple)
SELECT 'IN with Array form - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ['red', 'blue']
ORDER BY color_id, payload;

SELECT 'IN with Array form';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ['red', 'blue']
ORDER BY color_id, payload;

-- Test IN with Array form and default value
-- Re-add missing key for this test
INSERT INTO t VALUES (99, 'missing_key');

SELECT 'IN with Array form, default value - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ['', 'red']
ORDER BY color_id, payload;

SELECT 'IN with Array form, default value - result';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ['', 'red']
ORDER BY color_id, payload;

DELETE FROM t WHERE color_id = 99;

-- Test combination with AND
SELECT 'IN combined with AND - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
  AND dictGetUInt64('colors', 'n', color_id) < 10
ORDER BY color_id, payload;

SELECT 'IN combined with AND';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
  AND dictGetUInt64('colors', 'n', color_id) < 10
ORDER BY color_id, payload;

-- Test combination with OR
SELECT 'IN combined with OR - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
   OR dictGetString('colors', 'name', color_id) = 'green'
ORDER BY color_id, payload;

SELECT 'IN combined with OR';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN ('red', 'blue')
   OR dictGetString('colors', 'name', color_id) = 'green'
ORDER BY color_id, payload;

-- Negative: IN with non-constant tuple (subquery), expect no rewrite
SELECT 'Negative: IN with subquery - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN (SELECT 'red')
ORDER BY color_id, payload;

SELECT 'Negative: IN with subquery';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN (SELECT 'red')
ORDER BY color_id, payload;

-- Negative: IN with table column reference, expect no rewrite
SELECT 'Negative: IN with table column - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN (payload)
ORDER BY color_id, payload;

SELECT 'Negative: IN with table column';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) IN (payload)
ORDER BY color_id, payload;

-- Composite key dictionary tests
DROP DICTIONARY IF EXISTS dict_prices;
DROP TABLE IF EXISTS ref_prices;
DROP TABLE IF EXISTS fact;

CREATE TABLE ref_prices
(
    k1 UInt64,
    k2 String,
    price UInt64,
    tag String
)
ENGINE = MergeTree
ORDER BY (k1, k2);

INSERT INTO ref_prices VALUES
    (1, 'a', 100, 'pro'),
    (1, 'b',  50, 'basic'),
    (2, 'a',  75, 'plus'),
    (3, 'c',  10, 'cheap');

CREATE DICTIONARY dict_prices
(
  k1    UInt64,
  k2    String,
  price UInt64,
  tag   String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'ref_prices'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE TABLE fact
(
    k1 UInt64,
    k2 String,
    payload String
)
ENGINE = MergeTree
ORDER BY (k1, k2);

INSERT INTO fact VALUES
    (1, 'a', 'x'),
    (1, 'b', 'y'),
    (2, 'a', 'z'),
    (2, 'b', 'w'),
    (3, 'c', 'u');

-- Add a composite key that doesn't exist in the dictionary to test missing key behavior
INSERT INTO fact VALUES (99, 'z', 'missing_composite_key');

-- Test IN with composite key and default value in list
-- Default value for String (tag) is '', so this tests missing key handling
SELECT 'IN with composite key, default value - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) IN ('')
ORDER BY k1, k2, payload;

-- Missing key (99, 'z') should match because '' (default) IN ('') = true
SELECT 'IN with composite key, default value - result';
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) IN ('')
ORDER BY k1, k2, payload;

-- Test notIn with composite key and default value NOT in list
-- Missing key returns '', '' NOT IN ('pro', 'basic') = true, so missing keys should match
SELECT 'notIn with composite key, no default - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) NOT IN ('pro', 'basic')
ORDER BY k1, k2, payload;

-- Missing key (99, 'z') should match because '' NOT IN ('pro', 'basic') = true
SELECT 'notIn with composite key, no default - result';
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) NOT IN ('pro', 'basic')
ORDER BY k1, k2, payload;

-- Test IN with composite key, numeric attribute and default value
-- Default for UInt64 (price) is 0
SELECT 'IN with composite key, numeric default - plan (should have negated IN)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGetUInt64('dict_prices', 'price', (k1, k2)) IN (0)
ORDER BY k1, k2, payload;

-- Missing key (99, 'z') should match because 0 (default) IN (0) = true
SELECT 'IN with composite key, numeric default - result';
SELECT k1, k2, payload
FROM fact
WHERE dictGetUInt64('dict_prices', 'price', (k1, k2)) IN (0)
ORDER BY k1, k2, payload;

-- Clean up the extra row before other tests
DELETE FROM fact WHERE k1 = 99 AND k2 = 'z';

-- Test IN with composite key
SELECT 'IN with composite key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) IN ('pro', 'basic')
ORDER BY k1, k2, payload;

SELECT 'IN with composite key';
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) IN ('pro', 'basic')
ORDER BY k1, k2, payload;

-- Test notIn with composite key
SELECT 'notIn with composite key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) NOT IN ('pro', 'basic')
ORDER BY k1, k2, payload;

SELECT 'notIn with composite key';
SELECT k1, k2, payload
FROM fact
WHERE dictGet('dict_prices', 'tag', (k1, k2)) NOT IN ('pro', 'basic')
ORDER BY k1, k2, payload;

-- Test IN with composite key and numeric attribute
SELECT 'IN with composite key, numeric attribute - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM fact
WHERE dictGetUInt64('dict_prices', 'price', (k1, k2)) IN (100, 75)
ORDER BY k1, k2, payload;

SELECT 'IN with composite key, numeric attribute';
SELECT k1, k2, payload
FROM fact
WHERE dictGetUInt64('dict_prices', 'price', (k1, k2)) IN (100, 75)
ORDER BY k1, k2, payload;

-- Test dictGetOrNull: IN/notIn should NOT be rewritten (NULL semantics differ)
-- dictGetOrNull returns NULL for missing keys, but the inverse lookup rewrite
-- would return FALSE for missing keys, changing query semantics.
-- Re-add missing key for dictGetOrNull tests
INSERT INTO t VALUES (99, 'missing_key');

SELECT 'dictGetOrNull IN - plan (should NOT be rewritten)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetOrNull('colors', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'dictGetOrNull IN - result (missing key 99 returns NULL, filtered out)';
SELECT color_id, payload
FROM t
WHERE dictGetOrNull('colors', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'dictGetOrNull NOT IN - plan (should NOT be rewritten)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetOrNull('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'dictGetOrNull NOT IN - result (missing key 99 returns NULL, filtered out)';
SELECT color_id, payload
FROM t
WHERE dictGetOrNull('colors', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

-- Verify dictGetOrNull returns NULL for missing keys (baseline sanity check)
SELECT 'dictGetOrNull missing key sanity check';
SELECT color_id, dictGetOrNull('colors', 'name', color_id) AS name_val
FROM t
WHERE color_id IN (1, 99)
ORDER BY color_id;

-- Verify dictGet returns default for missing keys (contrast with dictGetOrNull)
SELECT 'dictGet missing key returns default';
SELECT color_id, dictGetString('colors', 'name', color_id) AS name_val
FROM t
WHERE color_id IN (1, 99)
ORDER BY color_id;

-- Test Nullable attribute: dictGet returns NULL for missing keys when the
-- attribute type is Nullable(...).  The IN/notIn rewrite must NOT fire because
-- it assumes two-valued logic (default value), which is wrong for NULL.
DROP TABLE IF EXISTS ref_colors_nullable;
CREATE TABLE ref_colors_nullable
(
    id UInt64,
    name Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO ref_colors_nullable VALUES
    (1, 'red'),
    (2, 'blue'),
    (3, 'red'),
    (4, 'green'),
    (5, 'Rose');

DROP DICTIONARY IF EXISTS colors_nullable;
CREATE DICTIONARY colors_nullable
(
  id   UInt64,
  name Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_colors_nullable'))
LAYOUT(HASHED())
LIFETIME(0);

DELETE FROM t WHERE color_id = 99;
INSERT INTO t VALUES (99, 'missing_key');

SELECT 'Nullable attribute: dictGet returns NULL for missing key';
SELECT color_id, dictGet('colors_nullable', 'name', color_id) AS name_val
FROM t
WHERE color_id IN (1, 99)
ORDER BY color_id;

SELECT 'Nullable attribute IN - plan (should NOT be rewritten)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGet('colors_nullable', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'Nullable attribute IN - result (missing key 99 → NULL, filtered out)';
SELECT color_id, payload
FROM t
WHERE dictGet('colors_nullable', 'name', color_id) IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'Nullable attribute NOT IN - plan (should NOT be rewritten)';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGet('colors_nullable', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

SELECT 'Nullable attribute NOT IN - result (missing key 99 → NULL, filtered out)';
SELECT color_id, payload
FROM t
WHERE dictGet('colors_nullable', 'name', color_id) NOT IN ('red', 'blue')
ORDER BY color_id, payload;

DROP DICTIONARY colors_nullable;
DROP TABLE ref_colors_nullable;

DROP DICTIONARY colors;
DROP DICTIONARY dict_prices;
DROP TABLE t;
DROP TABLE fact;
DROP TABLE ref_colors;
DROP TABLE ref_prices;
