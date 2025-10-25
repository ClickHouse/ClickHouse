-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
SET optimize_or_like_chain = 0;

DROP DICTIONARY IF EXISTS colors;
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

SELECT 'Equality, LHS - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, payload;

SELECT 'Equality, LHS';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, payload;

SELECT 'Equality, RHS - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE 'red' = dictGetString('colors', 'name', color_id)
ORDER BY color_id, payload;

SELECT 'Equality, RHS';
SELECT color_id, payload
FROM t
WHERE 'red' = dictGetString('colors', 'name', color_id)
ORDER BY color_id, payload;

SELECT 'Inequality <, LHS - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) < 10
ORDER BY color_id, payload;

SELECT 'Inequality <, LHS';
SELECT color_id, payload
FROM t
WHERE dictGetUInt64('colors', 'n', color_id) < 10
ORDER BY color_id, payload;

SELECT 'Inequality <, RHS - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE 10 > dictGetUInt64('colors', 'n', color_id)
ORDER BY color_id, payload;

SELECT 'Inequality <, RHS';
SELECT color_id, payload
FROM t
WHERE 10 > dictGetUInt64('colors', 'n', color_id)
ORDER BY color_id, payload;

SELECT 'Type variant cast, >= Int32 - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetInt32('colors', 'n', color_id) >= 2
ORDER BY color_id, payload;

SELECT 'Type variant cast, >= Int32';
SELECT color_id, payload
FROM t
WHERE dictGetInt32('colors', 'n', color_id) >= 2
ORDER BY color_id, payload;

SELECT 'LIKE - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) LIKE 'r%'
ORDER BY color_id, payload;

SELECT 'LIKE';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) LIKE 'r%'
ORDER BY color_id, payload;

SELECT 'ILIKE - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'ILIKE';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'NOT recursion - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE NOT (dictGetString('colors', 'name', color_id) = 'red')
ORDER BY color_id, payload;

SELECT 'NOT recursion';
SELECT color_id, payload
FROM t
WHERE NOT (dictGetString('colors', 'name', color_id) = 'red')
ORDER BY color_id, payload;

SELECT 'AND/OR recursion - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE (dictGetString('colors', 'name', color_id) = 'red' AND dictGetUInt64('colors', 'n', color_id) < 10)
   OR dictGetString('colors', 'name', color_id) = 'green'
ORDER BY color_id, payload;

SELECT 'AND/OR recursion';
SELECT color_id, payload
FROM t
WHERE (dictGetString('colors', 'name', color_id) = 'red' AND dictGetUInt64('colors', 'n', color_id) < 10)
   OR dictGetString('colors', 'name', color_id) = 'green'
ORDER BY color_id, payload;

SELECT 'NULL constant - plan';
EXPLAIN PLAN
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) = NULL
ORDER BY color_id, payload;

SELECT 'NULL constant';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) = NULL
ORDER BY color_id, payload;

SELECT 'PREWHERE - plan';
EXPLAIN PLAN
SELECT color_id
FROM t
PREWHERE dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id;

SELECT 'PREWHERE';
SELECT color_id
FROM t
PREWHERE dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id;

SELECT 'QUALIFY - plan';
EXPLAIN PLAN
SELECT color_id, row_number() OVER (PARTITION BY 1 ORDER BY color_id) AS rn
FROM t
QUALIFY dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, rn;

SELECT 'QUALIFY';
SELECT color_id, row_number() OVER (PARTITION BY 1 ORDER BY color_id) AS rn
FROM t
QUALIFY dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, rn;

-- Negative: non-constant RHS, expect no rewrite
SELECT 'Negative: non-constant RHS - plan';
EXPLAIN PLAN
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = payload
ORDER BY color_id;

SELECT 'Negative: non-constant RHS';
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = payload
ORDER BY color_id;
