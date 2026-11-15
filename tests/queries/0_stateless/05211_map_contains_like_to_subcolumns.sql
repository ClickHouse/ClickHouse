-- Map LIKE predicates should read only the matching Map subcolumn.

SET enable_analyzer = 1;
SET enable_identifier_resolve_cache = 1;
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
SELECT number + 10, 'unused%', 'unused%', map('a', '1', 'b', '2'), map('a', '1', 'b', '2')
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

-- Constant patterns must produce the same results on both paths, including
-- matches at the first and last positions, misses, empty Maps, and multi-element Maps.
DROP TABLE IF EXISTS t_map_contains_like_constants;

CREATE TABLE t_map_contains_like_constants
(
    id UInt8,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_constants VALUES
    (0, {'service': 'api', 'debug': '1'}),
    (1, {'debug': '1', 'service': 'api'}),
    (2, {'debug': '1', 'result': 'worker'}),
    (3, {}),
    (4, {'service': 'worker', 'result': 'api'});

SELECT id, mapContainsKeyLike(m, 'ser%'), mapContainsValueLike(m, 'api%')
FROM t_map_contains_like_constants
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id, mapContainsKeyLike(m, 'ser%'), mapContainsValueLike(m, 'api%')
FROM t_map_contains_like_constants
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_contains_like_constants;

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

-- A physical pattern column is safe to capture and should still be optimized.
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsKeyLike(m, pattern)
)
WHERE explain LIKE '%m.keys%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsValueLike(m, pattern)
)
WHERE explain LIKE '%m.values%';

-- A pattern captured from an enclosing lambda is not a storage column. Keep the original
-- Map LIKE implementation so the generated lambda cannot collide with the outer argument.
DROP TABLE IF EXISTS t_map_contains_like_lambda_capture;

CREATE TABLE t_map_contains_like_lambda_capture
(
    id UInt8,
    m Map(String, String),
    key_patterns Array(String),
    value_patterns Array(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_lambda_capture VALUES
    (1, {'alpha': 'one'}, ['a%', 'z%'], ['o%', 'z%']),
    (2, {}, ['a%', 'z%'], ['o%', 'z%']);

SELECT
    id,
    arrayMap(x -> mapContainsKeyLike(m, x), key_patterns) AS key_hits,
    arrayMap(x -> mapContainsValueLike(m, x), value_patterns) AS value_hits
FROM t_map_contains_like_lambda_capture
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT
    id,
    arrayMap(x -> mapContainsKeyLike(m, x), key_patterns) AS key_hits,
    arrayMap(x -> mapContainsValueLike(m, x), value_patterns) AS value_hits
FROM t_map_contains_like_lambda_capture
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_contains_like_lambda_capture;

-- FixedString Map elements should use the matching subcolumn too.
DROP TABLE IF EXISTS t_map_contains_like_fixed;

CREATE TABLE t_map_contains_like_fixed
(
    id UInt8,
    m_fixed_key Map(FixedString(3), String),
    m_fixed_value Map(String, FixedString(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_fixed VALUES
    (1, {'abc': 'one'}, {'key': 'val'}),
    (2, {'xyz': 'two'}, {'key': 'foo'});

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_fixed
    WHERE mapContainsKeyLike(m_fixed_key, 'a%')
)
WHERE explain LIKE '%m_fixed_key.keys%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_fixed
    WHERE mapContainsValueLike(m_fixed_value, 'v%')
)
WHERE explain LIKE '%m_fixed_value.values%';

SELECT count() = 1
FROM t_map_contains_like_fixed
WHERE mapContainsKeyLike(m_fixed_key, 'a%');

SELECT count() = 1
FROM t_map_contains_like_fixed
WHERE mapContainsKeyLike(m_fixed_key, 'a%')
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT count() = 1
FROM t_map_contains_like_fixed
WHERE mapContainsValueLike(m_fixed_value, 'v%');

SELECT count() = 1
FROM t_map_contains_like_fixed
WHERE mapContainsValueLike(m_fixed_value, 'v%')
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_contains_like_fixed;

-- Keep Map LIKE functions unchanged when the Map is required by a text index.
DROP TABLE IF EXISTS t_map_contains_like_text_index;

CREATE TABLE t_map_contains_like_text_index
(
    id UInt8,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_values mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_text_index VALUES
    (1, {'alpha': 'one'}),
    (2, {'beta': 'two'});

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_text_index
    WHERE mapContainsKeyLike(m, 'a%')
)
WHERE explain LIKE '%mapContainsKeyLike%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_text_index
    WHERE mapContainsKeyLike(m, 'a%')
)
WHERE explain LIKE '%arrayExists%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_text_index
    WHERE mapContainsValueLike(m, 'o%')
)
WHERE explain LIKE '%mapContainsValueLike%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_text_index
    WHERE mapContainsValueLike(m, 'o%')
)
WHERE explain LIKE '%arrayExists%';

DROP TABLE t_map_contains_like_text_index;

-- Expression patterns must not be moved into the synthesized lambda. This is a structural
-- check, so it does not depend on the output of a non-deterministic function.
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsKeyLike(m, if(rand() % 2 = 0, 'a%', 'b%'))
)
WHERE explain LIKE '%m.keys%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_subcolumns
    WHERE mapContainsValueLike(m, if(rand() % 2 = 0, '1%', '2%'))
)
WHERE explain LIKE '%m.values%';

-- Expression-backed ALIAS columns must also stay outside the synthesized lambda in PREWHERE.
DROP TABLE IF EXISTS t_map_contains_like_alias;

CREATE TABLE t_map_contains_like_alias
(
    d UInt8,
    m Map(String, String),
    pattern String ALIAS toString(intDiv(1, d))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_map_contains_like_alias (d, m) VALUES (0, {});

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT d
    FROM t_map_contains_like_alias
    PREWHERE mapContainsKeyLike(m, pattern)
)
WHERE explain LIKE '%m.keys%';

SELECT count()
FROM t_map_contains_like_alias
PREWHERE mapContainsKeyLike(m, pattern)
SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError ILLEGAL_DIVISION }

SELECT count()
FROM t_map_contains_like_alias
PREWHERE mapContainsKeyLike(m, pattern)
SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError ILLEGAL_DIVISION }

DROP TABLE t_map_contains_like_alias;

-- Nullable patterns must keep NULL results. arrayExists would otherwise turn a NULL lambda
-- result into false, which is also visible in a NOT predicate.
DROP TABLE IF EXISTS t_map_contains_like_nullable_pattern;

CREATE TABLE t_map_contains_like_nullable_pattern
(
    id UInt8,
    m Map(String, String),
    pattern Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_nullable_pattern VALUES
    (1, {'service': 'api'}, NULL),
    (2, {'service': 'api'}, 'ser%'),
    (3, {}, NULL),
    (4, {'k': 'value'}, 'value%');

SELECT id, mapContainsKeyLike(m, pattern)
FROM t_map_contains_like_nullable_pattern
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id, mapContainsKeyLike(m, pattern)
FROM t_map_contains_like_nullable_pattern
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id, mapContainsValueLike(m, pattern)
FROM t_map_contains_like_nullable_pattern
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id, mapContainsValueLike(m, pattern)
FROM t_map_contains_like_nullable_pattern
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_contains_like_nullable_pattern
WHERE NOT mapContainsKeyLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id
FROM t_map_contains_like_nullable_pattern
WHERE NOT mapContainsKeyLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_contains_like_nullable_pattern
WHERE NOT mapContainsValueLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id
FROM t_map_contains_like_nullable_pattern
WHERE NOT mapContainsValueLike(m, pattern)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_contains_like_nullable_pattern;

-- LowCardinality on the searched Map element or pattern keeps the original Map LIKE implementation.
SELECT countIf(mapContainsKeyLike(m_key_lc, 'ser%')) = 3
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsKeyLike(m_key_lc, 'ser%')) = 3
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT countIf(mapContainsKeyLike(m, pattern_lc)) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(mapContainsKeyLike(m, pattern_lc)) = 2
FROM t_map_contains_like_subcolumns
SETTINGS optimize_functions_to_subcolumns = 0;

-- LowCardinality on the unused Map element does not block the rewrite.
DROP TABLE IF EXISTS t_map_contains_like_unused_lc;

CREATE TABLE t_map_contains_like_unused_lc
(
    id UInt8,
    m_value_lc Map(String, LowCardinality(String)),
    m_key_lc Map(LowCardinality(String), String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_contains_like_unused_lc VALUES
    (0, {'service': 'api'}, {'service': 'api'}),
    (1, {}, {}),
    (2, {'other': 'worker'}, {'other': 'worker'});

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_unused_lc
    WHERE mapContainsKeyLike(m_value_lc, 'ser%')
)
WHERE explain LIKE '%m_value_lc.keys%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM t_map_contains_like_unused_lc
    WHERE mapContainsValueLike(m_key_lc, 'api%')
)
WHERE explain LIKE '%m_key_lc.values%';

SELECT id, mapContainsKeyLike(m_value_lc, 'ser%')
FROM t_map_contains_like_unused_lc
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id, mapContainsKeyLike(m_value_lc, 'ser%')
FROM t_map_contains_like_unused_lc
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id, mapContainsValueLike(m_key_lc, 'api%')
FROM t_map_contains_like_unused_lc
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id, mapContainsValueLike(m_key_lc, 'api%')
FROM t_map_contains_like_unused_lc
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_contains_like_unused_lc;

-- Reusing a scalar Map LIKE alias must not mutate the shared resolved function node.
WITH mapContainsKeyLike(m, 'd%') AS hit
SELECT id, hit, NOT hit
FROM t_map_contains_like_subcolumns
WHERE id < 3
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

WITH mapContainsKeyLike(m, 'd%') AS hit
SELECT id, hit, NOT hit
FROM t_map_contains_like_subcolumns
WHERE id < 3
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

WITH mapContainsValueLike(m, 'a%') AS hit
SELECT id, hit, NOT hit
FROM t_map_contains_like_subcolumns
WHERE id < 3
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

WITH mapContainsValueLike(m, 'a%') AS hit
SELECT id, hit, NOT hit
FROM t_map_contains_like_subcolumns
WHERE id < 3
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_contains_like_subcolumns;
