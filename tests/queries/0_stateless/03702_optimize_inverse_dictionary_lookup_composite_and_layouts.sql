-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_or_like_chain = 0;

DROP DICTIONARY IF EXISTS dict_prices_ckh;
DROP DICTIONARY IF EXISTS dict_items_flat;
DROP DICTIONARY IF EXISTS dict_salary_rh;
DROP TABLE IF EXISTS ref_prices_ckh;
DROP TABLE IF EXISTS ref_items_flat;
DROP TABLE IF EXISTS ref_salary_rh;
DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS g;

CREATE TABLE ref_prices_ckh
(
    k1 UInt64,
    k2 String,
    price UInt64,
    tag String
)
ENGINE = MergeTree
ORDER BY (k1, k2);

INSERT INTO ref_prices_ckh VALUES
    (1, 'a', 100, 'pro'),
    (1, 'b',  50, 'basic'),
    (2, 'a',  75, 'plus'),
    (3, 'c',  10, 'cheap');

CREATE TABLE ref_items_flat
(
    id UInt64,
    name String,
    score UInt64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO ref_items_flat VALUES
    (1, 'alpha', 10),
    (2, 'beta',   5),
    (3, 'alpha', 15);

CREATE TABLE ref_salary_rh
(
    id UInt64,
    dt_from Date,
    dt_to Date,
    rate UInt64
)
ENGINE = MergeTree
ORDER BY (id, dt_from);

INSERT INTO ref_salary_rh VALUES
    (1, toDate('2024-01-01'), toDate('2024-01-31'), 100),
    (1, toDate('2024-02-01'), toDate('2024-12-31'), 200),
    (2, toDate('2024-01-01'), toDate('2024-12-31'),  75);

CREATE DICTIONARY dict_prices_ckh
(
  k1     UInt64,
  k2     String,
  price  UInt64,
  tag    String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'ref_prices_ckh'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_items_flat
(
  id    UInt64,
  name  String,
  score UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_items_flat'))
LAYOUT(FLAT())
LIFETIME(0);

CREATE DICTIONARY dict_salary_rh
(
  id      UInt64,
  dt_from Date,
  dt_to   Date,
  rate    UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_salary_rh'))
LAYOUT(RANGE_HASHED())
RANGE(MIN dt_from MAX dt_to)
LIFETIME(0);

CREATE TABLE f
(
    k1 UInt64,
    k2 String,
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY (k1, k2, id);

INSERT INTO f VALUES
    (1, 'a', 1, 'x'),
    (1, 'b', 2, 'y'),
    (2, 'a', 3, 'z'),
    (2, 'b', 2, 'w'),
    (3, 'c', 1, 'u');

CREATE TABLE g
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO g VALUES (1,'p'),(2,'q');

SELECT 'Composite key - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'Composite key';
SELECT k1, k2, payload
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'Composite key: AND/OR recursion - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2
FROM f
WHERE (dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
       AND dictGetInt32('dict_prices_ckh', 'price', (k1, k2)) < 120)
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'cheap'
ORDER BY k1, k2;

SELECT 'Composite key: AND/OR recursion';
SELECT k1, k2
FROM f
WHERE (dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
       AND dictGetInt32('dict_prices_ckh', 'price', (k1, k2)) < 120)
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'cheap'
ORDER BY k1, k2;

SELECT 'Composite key, Complex Key Hashed layout: MATCH - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2
FROM f
WHERE match(dictGetString('dict_prices_ckh', 'tag', (k1, k2)), '^p')
ORDER BY k1, k2;

SELECT 'Composite key, Complex Key Hashed layout: MATCH';
SELECT k1, k2
FROM f
WHERE match(dictGetString('dict_prices_ckh', 'tag', (k1, k2)), '^p')
ORDER BY k1, k2;

SELECT 'Flat layout: equality and inequality - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM f
WHERE dictGetString('dict_items_flat', 'name', id) = 'alpha'
  AND dictGetUInt64('dict_items_flat', 'score', id) >= 10
ORDER BY id, payload;

SELECT 'Flat layout: equality and inequality';
SELECT id, payload
FROM f
WHERE dictGetString('dict_items_flat', 'name', id) = 'alpha'
  AND dictGetUInt64('dict_items_flat', 'score', id) >= 10
ORDER BY id, payload;

SELECT 'Range layout: not supported, no rewrite expected - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM g
WHERE dictGetUInt64('dict_salary_rh', 'rate', id, toDate('2024-01-15')) >= 100
ORDER BY id, payload;

SELECT 'Range layout: not supported, no rewrite expected';
SELECT id, payload
FROM g
WHERE dictGetUInt64('dict_salary_rh', 'rate', id, toDate('2024-01-15')) >= 100
ORDER BY id, payload;
