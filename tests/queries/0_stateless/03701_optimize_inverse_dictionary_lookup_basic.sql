-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'ILIKE';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors', 'name', color_id) ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'equals() - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id
FROM t
WHERE equals(dictGetString('colors','name', color_id), 'red')
ORDER BY color_id;

SELECT 'equals()';
SELECT color_id
FROM t
WHERE equals(dictGetString('colors','name', color_id), 'red')
ORDER BY color_id;

SELECT 'notEquals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) != 'red'
ORDER BY color_id, payload;

SELECT 'notEquals';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) != 'red'
ORDER BY color_id, payload;

SELECT 'NOT LIKE r% - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) NOT LIKE 'r%'
ORDER BY color_id, payload;

SELECT 'NOT LIKE r%';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) NOT LIKE 'r%'
ORDER BY color_id, payload;

SELECT 'NOT ILIKE r% - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) NOT ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'NOT ILIKE r%';
SELECT color_id, payload
FROM t
WHERE dictGetString('colors','name', color_id) NOT ILIKE 'r%'
ORDER BY color_id, payload;

SELECT 'match ^r - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
WHERE match(dictGetString('colors','name', color_id), '^r')
ORDER BY color_id, payload;

SELECT 'match ^r';
SELECT color_id, payload
FROM t
WHERE match(dictGetString('colors','name', color_id), '^r')
ORDER BY color_id, payload;

SELECT 'NOT recursion - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
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
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, row_number() OVER (PARTITION BY 1 ORDER BY color_id) AS rn
FROM t
QUALIFY dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, rn;

SELECT 'QUALIFY';
SELECT color_id, row_number() OVER (PARTITION BY 1 ORDER BY color_id) AS rn
FROM t
QUALIFY dictGetString('colors', 'name', color_id) = 'red'
ORDER BY color_id, rn;

SELECT 'Empty result set - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = 'nonexistent_color'
ORDER BY color_id;

SELECT 'Empty result set';
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = 'nonexistent_color'
ORDER BY color_id;

SELECT 'HAVING - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, count() AS c
FROM t
GROUP BY color_id
HAVING dictGetString('colors','name', color_id) = 'red'
ORDER BY color_id, c;

SELECT 'HAVING';
SELECT color_id, count() AS c
FROM t
GROUP BY color_id
HAVING dictGetString('colors','name', color_id) = 'red'
ORDER BY color_id, c;

SELECT 'JOIN ON (INNER) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT t1.color_id, t1.payload, t2.payload AS payload2
FROM t AS t1
INNER JOIN t AS t2
  ON t1.color_id = t2.color_id
 AND dictGetString('colors','name', t1.color_id) = 'red'
ORDER BY t1.color_id, t1.payload, payload2;

SELECT 'JOIN ON (INNER)';
SELECT t1.color_id, t1.payload, t2.payload AS payload2
FROM t AS t1
INNER JOIN t AS t2
  ON t1.color_id = t2.color_id
 AND dictGetString('colors','name', t1.color_id) = 'red'
ORDER BY t1.color_id, t1.payload, payload2;

SELECT 'JOIN ON (LEFT) - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT t1.color_id, t1.payload, t2.payload AS payload2
FROM t AS t1
LEFT JOIN t AS t2
  ON t1.color_id = t2.color_id
 AND dictGetString('colors','name', t1.color_id) = 'red'
ORDER BY t1.color_id, t1.payload, payload2;

SELECT 'JOIN ON (LEFT)';
SELECT t1.color_id, t1.payload, t2.payload AS payload2
FROM t AS t1
LEFT JOIN t AS t2
  ON t1.color_id = t2.color_id
 AND dictGetString('colors','name', t1.color_id) = 'red'
ORDER BY t1.color_id, t1.payload, payload2;

SELECT 'SELECT multiIf - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload,
       multiIf(dictGetString('colors','name', color_id) = 'red', 'match', 'no_match') AS tag
FROM t
ORDER BY color_id, payload, tag;

SELECT 'SELECT multiIf';
SELECT color_id, payload,
       multiIf(dictGetString('colors','name', color_id) = 'red', 'match', 'no_match') AS tag
FROM t
ORDER BY color_id, payload, tag;

SELECT 'countIf - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT countIf(dictGetString('colors','name', color_id) = 'red') AS cnt
FROM t;

SELECT 'countIf';
SELECT countIf(dictGetString('colors','name', color_id) = 'red') AS cnt
FROM t;

SELECT 'sumIf - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT sumIf(color_id, dictGetString('colors','name', color_id) = 'red') AS sum_id_match
FROM t;

SELECT 'sumIf';
SELECT sumIf(color_id, dictGetString('colors','name', color_id) = 'red') AS sum_id_match
FROM t;

SELECT 'ORDER BY - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
ORDER BY (dictGetString('colors','name', color_id) = 'red') DESC, color_id, payload;

SELECT 'ORDER BY';
SELECT color_id, payload
FROM t
ORDER BY (dictGetString('colors','name', color_id) = 'red') DESC, color_id, payload;

SELECT 'GROUP BY - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT (dictGetString('colors','name', color_id) = 'red') AS is_red, count() AS c
FROM t
GROUP BY (dictGetString('colors','name', color_id) = 'red')
ORDER BY is_red, c;

SELECT 'GROUP BY';
SELECT (dictGetString('colors','name', color_id) = 'red') AS is_red, count() AS c
FROM t
GROUP BY (dictGetString('colors','name', color_id) = 'red')
ORDER BY is_red, c;

SELECT 'LIMIT BY - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id, payload
FROM t
ORDER BY color_id, payload
LIMIT 1 BY (dictGetString('colors','name', color_id) = 'red');

SELECT 'LIMIT BY';
SELECT color_id, payload
FROM t
ORDER BY color_id, payload
LIMIT 1 BY (dictGetString('colors','name', color_id) = 'red');

SELECT 'WINDOW PARTITION BY - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id,
       row_number() OVER (
           PARTITION BY (dictGetString('colors','name', color_id) = 'red')
           ORDER BY color_id
       ) AS rn
FROM t
ORDER BY color_id, rn;

SELECT 'WINDOW PARTITION BY';
SELECT color_id,
       row_number() OVER (
           PARTITION BY (dictGetString('colors','name', color_id) = 'red')
           ORDER BY color_id
       ) AS rn
FROM t
ORDER BY color_id, rn;

-- Negative: non-constant RHS, expect no rewrite
SELECT 'Negative: non-constant RHS - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = payload
ORDER BY color_id;

SELECT 'Negative: non-constant RHS';
SELECT color_id
FROM t
WHERE dictGetString('colors', 'name', color_id) = payload
ORDER BY color_id;
