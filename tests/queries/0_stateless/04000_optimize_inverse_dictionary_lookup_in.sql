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

DROP DICTIONARY colors;
DROP DICTIONARY dict_prices;
DROP TABLE t;
DROP TABLE fact;
DROP TABLE ref_colors;
DROP TABLE ref_prices;
