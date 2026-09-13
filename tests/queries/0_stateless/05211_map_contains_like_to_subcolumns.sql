-- Map LIKE predicates should read only the matching Map subcolumn.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_contains_like_subcolumns;

CREATE TABLE t_map_contains_like_subcolumns
(
    id UInt64,
    pattern String,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_subcolumns VALUES
    (0, 'ser%', {'service': 'api', 'debug': '1'}),
    (1, 'ser%', {'service': 'worker'}),
    (2, 'ser%', {'debug': '1'}),
    (3, 'api%', {}),
    (4, 'api%', {'service': 'api'});

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

-- Captured, non-constant patterns must keep the same results.
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

DROP TABLE t_map_contains_like_subcolumns;
