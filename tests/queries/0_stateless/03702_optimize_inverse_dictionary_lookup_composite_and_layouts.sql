-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_or_like_chain = 0;

DROP DICTIONARY IF EXISTS dict_prices_ckh;
DROP DICTIONARY IF EXISTS dict_items_flat;
DROP TABLE IF EXISTS ref_prices_ckh;
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

DROP TABLE IF EXISTS ref_items_flat;
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

DROP DICTIONARY IF EXISTS dict_prices_ckh;
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

DROP DICTIONARY IF EXISTS dict_items_flat;
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

DROP TABLE IF EXISTS f;
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

SELECT 'Composite key: equality, LHS - plan';
EXPLAIN PLAN
SELECT k1, k2, payload
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'Composite key: equality, LHS - run';
SELECT k1, k2, payload
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'Composite key: equality, RHS - plan';
EXPLAIN PLAN
SELECT k1, k2, payload
FROM f
WHERE 'pro' = dictGetString('dict_prices_ckh', 'tag', (k1, k2))
ORDER BY k1, k2, payload;

SELECT 'Composite key: equality, RHS - run';
SELECT k1, k2, payload
FROM f
WHERE 'pro' = dictGetString('dict_prices_ckh', 'tag', (k1, k2))
ORDER BY k1, k2, payload;

SELECT 'Composite key: inequality <, LHS - plan';
EXPLAIN PLAN
SELECT k1, k2, payload
FROM f
WHERE dictGetUInt64('dict_prices_ckh', 'price', (k1, k2)) < 60
ORDER BY k1, k2, payload;

SELECT 'Composite key: inequality <, LHS - run';
SELECT k1, k2, payload
FROM f
WHERE dictGetUInt64('dict_prices_ckh', 'price', (k1, k2)) < 60
ORDER BY k1, k2, payload;

SELECT 'Composite key: inequality <, RHS - plan';
EXPLAIN PLAN
SELECT k1, k2, payload
FROM f
WHERE 60 > dictGetUInt64('dict_prices_ckh', 'price', (k1, k2))
ORDER BY k1, k2, payload;

SELECT 'Composite key: inequality <, RHS - run';
SELECT k1, k2, payload
FROM f
WHERE 60 > dictGetUInt64('dict_prices_ckh', 'price', (k1, k2))
ORDER BY k1, k2, payload;

SELECT 'Composite key: LIKE/ILIKE - plan';
EXPLAIN PLAN
SELECT k1, k2
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) LIKE 'p%'
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) ILIKE 'P%'
ORDER BY k1, k2;

SELECT 'Composite key: LIKE/ILIKE - run';
SELECT k1, k2
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) LIKE 'p%'
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) ILIKE 'P%'
ORDER BY k1, k2;

SELECT 'Composite key: AND/OR recursion - plan';
EXPLAIN PLAN
SELECT k1, k2
FROM f
WHERE (dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
       AND dictGetInt32('dict_prices_ckh', 'price', (k1, k2)) < 120)
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'cheap'
ORDER BY k1, k2;

SELECT 'Composite key: AND/OR recursion - run';
SELECT k1, k2
FROM f
WHERE (dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
       AND dictGetInt32('dict_prices_ckh', 'price', (k1, k2)) < 120)
   OR dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'cheap'
ORDER BY k1, k2;

SELECT 'Composite key: PREWHERE - plan';
EXPLAIN PLAN
SELECT k1, k2
FROM f
PREWHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'plus'
ORDER BY k1, k2;

SELECT 'Composite key: PREWHERE - run';
SELECT k1, k2
FROM f
PREWHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) = 'plus'
ORDER BY k1, k2;

SELECT 'Flat layout: equality and inequality - plan';
EXPLAIN PLAN
SELECT id, payload
FROM f
WHERE dictGetString('dict_items_flat', 'name', id) = 'alpha'
  AND dictGetUInt64('dict_items_flat', 'score', id) >= 10
ORDER BY id, payload;

SELECT 'Flat layout: equality and inequality - run';
SELECT id, payload
FROM f
WHERE dictGetString('dict_items_flat', 'name', id) = 'alpha'
  AND dictGetUInt64('dict_items_flat', 'score', id) >= 10
ORDER BY id, payload;

SELECT 'Composite key: NOT LIKE - plan';
EXPLAIN PLAN
SELECT k1, k2
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) NOT LIKE 'b%'
ORDER BY k1, k2;

SELECT 'Composite key: NOT LIKE - run';
SELECT k1, k2
FROM f
WHERE dictGetString('dict_prices_ckh', 'tag', (k1, k2)) NOT LIKE 'b%'
ORDER BY k1, k2;

SELECT 'Composite key: MATCH - plan';
EXPLAIN PLAN
SELECT k1, k2
FROM f
WHERE match(dictGetString('dict_prices_ckh', 'tag', (k1, k2)), '^p')
ORDER BY k1, k2;

SELECT 'Composite key: MATCH - run';
SELECT k1, k2
FROM f
WHERE match(dictGetString('dict_prices_ckh', 'tag', (k1, k2)), '^p')
ORDER BY k1, k2;
