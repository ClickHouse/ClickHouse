-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_or_like_chain = 0;

DROP DICTIONARY IF EXISTS dict_prices_ckh;
DROP DICTIONARY IF EXISTS dict_prices_ch_array;
DROP DICTIONARY IF EXISTS dict_prices_ck_sparse_hashed;
DROP DICTIONARY IF EXISTS dict_items_flat;
DROP DICTIONARY IF EXISTS dict_items_hashed;
DROP DICTIONARY IF EXISTS dict_items_hashed_array;
DROP DICTIONARY IF EXISTS dict_items_sparse_hashed;

DROP TABLE IF EXISTS ref_prices_ckh;
DROP TABLE IF EXISTS ref_items_flat;
DROP TABLE IF EXISTS f;

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

CREATE DICTIONARY dict_prices_ckh
(
  k1    UInt64,
  k2    String,
  price UInt64,
  tag   String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'ref_prices_ckh'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_prices_ch_array
(
  k1    UInt64,
  k2    String,
  price UInt64,
  tag   String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'ref_prices_ckh'))
LAYOUT(COMPLEX_KEY_HASHED_ARRAY())
LIFETIME(0);

CREATE DICTIONARY dict_prices_ck_sparse_hashed
(
  k1    UInt64,
  k2    String,
  price UInt64,
  tag   String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'ref_prices_ckh'))
LAYOUT(COMPLEX_KEY_SPARSE_HASHED())
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

CREATE DICTIONARY dict_items_hashed
(
  id    UInt64,
  name  String,
  score UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_items_flat'))
LAYOUT(HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_items_hashed_array
(
  id    UInt64,
  name  String,
  score UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_items_flat'))
LAYOUT(HASHED_ARRAY())
LIFETIME(0);

CREATE DICTIONARY dict_items_sparse_hashed
(
  id    UInt64,
  name  String,
  score UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_items_flat'))
LAYOUT(SPARSE_HASHED())
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

SELECT 'ComplexKeyHashed - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'ComplexKeyHashed';
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ckh', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'ComplexHashedArray - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ch_array', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'ComplexHashedArray';
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ch_array', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'ComplexKeySparseHashed - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ck_sparse_hashed', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;
SELECT 'ComplexKeySparseHashed';
SELECT k1, k2, payload
FROM f
WHERE dictGet('dict_prices_ck_sparse_hashed', 'tag', (k1, k2)) = 'pro'
ORDER BY k1, k2, payload;

SELECT 'Flat - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM f
WHERE dictGet('dict_items_flat', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'Flat';
SELECT id, payload
FROM f
WHERE dictGet('dict_items_flat', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'Hashed - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM f
WHERE dictGet('dict_items_hashed', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'Hashed';
SELECT id, payload
FROM f
WHERE dictGet('dict_items_hashed', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'HashedArray - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM f
WHERE dictGet('dict_items_hashed_array', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'HashedArray';
SELECT id, payload
FROM f
WHERE dictGet('dict_items_hashed_array', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'SparseHashed - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT id, payload
FROM f
WHERE dictGet('dict_items_sparse_hashed', 'name', id) = 'alpha'
ORDER BY id, payload;

SELECT 'SparseHashed';
SELECT id, payload
FROM f
WHERE dictGet('dict_items_sparse_hashed', 'name', id) = 'alpha'
ORDER BY id, payload;
