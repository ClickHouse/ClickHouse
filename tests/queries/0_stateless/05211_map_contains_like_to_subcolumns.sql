-- Map LIKE predicates should read only the matching Map subcolumn.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_contains_like_subcolumns;

CREATE TABLE t_map_contains_like_subcolumns
(
    id UInt64,
    pattern String,
    pattern_lc LowCardinality(String),
    m Map(String, String),
    m_key_lc Map(LowCardinality(String), String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_subcolumns VALUES
    (0, 'ser%', 'ser%', {'service': 'api', 'debug': '1'}, {'service': 'api', 'debug': '1'}),
    (1, 'ser%', 'ser%', {'service': 'worker'}, {'service': 'worker'}),
    (2, 'ser%', 'ser%', {'debug': '1'}, {'debug': '1'}),
    (3, 'api%', 'api%', {}, {}),
    (4, 'api%', 'api%', {'service': 'api'}, {'service': 'api'}),
    (5, 'unused%', 'unused%', {'a': '1', 'b': '2'}, {'a': '1', 'b': '2'});

INSERT INTO t_map_contains_like_subcolumns
SELECT number + 10, 'unused%', 'unused%', {'a': '1', 'b': '2'}, {'a': '1', 'b': '2'}
FROM numbers(10000);

-- A key-only predicate should use m.keys.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsKeyLike(m, 'ser%')
)
WHERE explain LIKE '%m.keys%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsKeyLike(m, 'ser%')
)
WHERE explain LIKE '%m.values%';

-- A value-only predicate should use m.values.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsValueLike(m, 'w%')
)
WHERE explain LIKE '%m.values%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsValueLike(m, 'w%')
)
WHERE explain LIKE '%m.keys%';

-- The full Map can still be selected while the filter reads one side only.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT m
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsKeyLike(m, 'ser%')
)
WHERE explain LIKE '%m.keys%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT m
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsValueLike(m, 'w%')
)
WHERE explain LIKE '%m.values%';

-- Captured column patterns must keep the same results.
SELECT id
FROM t_map_contains_like_subcolumns
WHERE mapContainsKeyLike(m, pattern)
ORDER BY id;

SELECT id
FROM t_map_contains_like_subcolumns
WHERE mapContainsKeyLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id
FROM t_map_contains_like_subcolumns
WHERE mapContainsValueLike(m, pattern)
ORDER BY id;

SELECT id
FROM t_map_contains_like_subcolumns
WHERE mapContainsValueLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

-- Expression patterns must keep their input-row evaluation scope. If the expression were moved
-- into the synthesized lambda, rand() could be evaluated once per Map element instead.
SELECT countIf(mapContainsKeyLike(m, if(rand() % 2 = 0, 'a%', 'b%'))) = count()
FROM t_map_contains_like_subcolumns
WHERE id >= 10
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsValueLike(m, if(rand() % 2 = 0, '1%', '2%'))) = count()
FROM t_map_contains_like_subcolumns
WHERE id >= 10
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsKeyLike(m, if(rand() % 2 = 0, 'a%', 'b%'))) = count()
FROM t_map_contains_like_subcolumns
WHERE id >= 10
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT countIf(mapContainsValueLike(m, if(rand() % 2 = 0, '1%', '2%'))) = count()
FROM t_map_contains_like_subcolumns
WHERE id >= 10
SETTINGS optimize_functions_to_subcolumns = 0;

-- LowCardinality Map elements and patterns stay on the original Map LIKE implementation.
SELECT countIf(mapContainsKeyLike(m_key_lc, 'ser%')) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsKeyLike(m_key_lc, 'ser%')) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT countIf(mapContainsKeyLike(m, pattern_lc)) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsKeyLike(m, pattern_lc)) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_contains_like_subcolumns;
