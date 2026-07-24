-- Tags: no-fasttest, no-parallel-replicas
-- Test: text (inverted/GIN) skip index support for JSONAllPaths on JSON data type
--
-- Data layout: 2 parts x 2 granules each (index_granularity = 1, ORDER BY tuple()).
--   Part 1: (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}')
--   Part 2: (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}')

-- =============================================================================
-- Section 1: Equals
-- =============================================================================

DROP TABLE IF EXISTS t_json_text;
CREATE TABLE t_json_text
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_text VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_text VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- 1a: Dynamic equals
SELECT 'text equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1b: Non-existing path
SELECT 'text non-existing';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1c: Typed subcolumn .:Int64
SELECT 'text typed subcolumn';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b.:Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1d: CAST ::Int64 = 1
SELECT 'text CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b::Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1e: CAST ::Int64 = 0 — unsafe
SELECT 'text CAST default unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b::Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1f: Typed subcolumn .:Int64 = 0 — safe (Nullable)
SELECT 'text typed zero safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b.:Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1g: Reversed argument order
SELECT 'text reversed equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE 1 = json.a.b)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 2: IN — path-existence filtering via traverseJSONSubcolumnKeyNode
-- =============================================================================

-- 2a: IN with typed subcolumn — path-existence skip works
SELECT 'text IN typed subcolumn';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b.:Int64 IN (1, 2, 3))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2b: IN with subquery — set must be built before evaluation
SELECT 'text IN subquery';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b.:Int64 IN (SELECT number FROM numbers(3)))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2c: IN with CAST containing zero — unsafe (CAST default is 0, which is in the set)
SELECT 'text IN CAST with zero unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b::Int64 IN (0, 1, 2))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 3: traverseJSONSubcolumnKeyNode — arbitrary boolean functions
-- =============================================================================

-- 3a: IS NOT NULL on typed subcolumn (NULL IS NOT NULL is false → safe)
SELECT 'text IS NOT NULL safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b.:Int64 IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3b: IS NOT NULL with CAST to non-Nullable type — unsafe, index should NOT be used
SELECT 'text IS NOT NULL CAST unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE isNotNull(json.a.b::Int64))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 4: Sub-object ^ access
-- =============================================================================

-- 4a: Sub-object access — index should NOT be used
SELECT 'text sub-object ^ not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE empty(json.^a))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 5: AND/OR
-- =============================================================================

-- 5a: AND — both paths in part 1
SELECT 'text AND both paths';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b = 1 AND json.c = 'hello')
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 5b: OR — path a.b in part 1, path x.y in part 2
SELECT 'text OR different paths';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b = 1 OR json.x.y = 3)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 5c: AND — one non-existing
SELECT 'text AND one non-existing';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text WHERE json.a.b = 1 AND json.nonexistent = 99)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 6: Dotted column name and Tuple(JSON)
-- =============================================================================

DROP TABLE t_json_text;

DROP TABLE IF EXISTS t_json_text_dotted;
CREATE TABLE t_json_text_dotted
(
    id UInt64,
    `my.json` JSON,
    INDEX idx JSONAllPaths(`my.json`) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_text_dotted VALUES (1, '{"path": 1}'), (2, '{"other": 2}');
INSERT INTO t_json_text_dotted VALUES (3, '{"third": 3}'), (4, '{"fourth": 4}');

SELECT 'text dotted column name';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text_dotted WHERE `my.json`.path = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_text_dotted;

DROP TABLE IF EXISTS t_json_text_tuple;
CREATE TABLE t_json_text_tuple
(
    id UInt64,
    t Tuple(json JSON),
    INDEX idx JSONAllPaths(t.json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_text_tuple VALUES (1, tuple('{"a": 1}')), (2, tuple('{"b": 2}'));
INSERT INTO t_json_text_tuple VALUES (3, tuple('{"c": 3}')), (4, tuple('{"d": 4}'));

SELECT 'text Tuple(JSON) subcolumn';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_text_tuple WHERE t.json.a = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_text_tuple;

-- =============================================================================
-- Section 7: Correctness
-- =============================================================================

DROP TABLE IF EXISTS t_json_text_res;
CREATE TABLE t_json_text_res
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_text_res VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_text_res VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- Single-path
SELECT 'result: text equals';
SELECT id FROM t_json_text_res WHERE json.a.b = 1 ORDER BY id;

SELECT 'result: text non-existing';
SELECT id FROM t_json_text_res WHERE json.nonexistent = 1 ORDER BY id;

SELECT 'result: text CAST';
SELECT id FROM t_json_text_res WHERE json.a.b::Int64 = 1 ORDER BY id;

-- Multi-path
SELECT 'result: text AND';
SELECT id FROM t_json_text_res WHERE json.a.b = 1 AND json.c = 'hello' ORDER BY id;

SELECT 'result: text OR';
SELECT id FROM t_json_text_res WHERE json.a.b = 1 OR json.x.y = 3 ORDER BY id;

SELECT 'result: text AND nonexistent';
SELECT id FROM t_json_text_res WHERE json.a.b = 1 AND json.nonexistent = 99 ORDER BY id;

-- IN
SELECT 'result: text IN';
SELECT id FROM t_json_text_res WHERE json.a.b.:Int64 IN (1, 2, 3) ORDER BY id;

SELECT 'result: text IN subquery';
SELECT id FROM t_json_text_res WHERE json.a.b.:Int64 IN (SELECT number FROM numbers(3)) ORDER BY id;

DROP TABLE t_json_text_res;
