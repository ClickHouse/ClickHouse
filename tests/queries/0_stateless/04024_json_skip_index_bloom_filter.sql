-- Tags: no-fasttest, no-parallel-replicas
-- Test: bloom_filter skip index support for JSONAllPaths on JSON data type
--
-- Data layout: 2 parts x 2 granules each (index_granularity = 1, ORDER BY tuple()).
--   Part 1: (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}')
--   Part 2: (3, '{"x": {"y": 3}, "z": "test"}'),  (4, '{"p": {"q": 4}, "r": "foo"}')

-- =============================================================================
-- Section 1: Equals
-- =============================================================================

DROP TABLE IF EXISTS t_json_bf;
CREATE TABLE t_json_bf
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_bf VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_bf VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- 1a: Dynamic subcolumn equals — path a.b only in part 1, granule 1
SELECT 'bloom_filter equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1b: Non-existing path — skip everything
SELECT 'bloom_filter non-existing path';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1c: Typed subcolumn .:Int64 (Nullable result)
SELECT 'bloom_filter typed subcolumn';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b.:Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1d: CAST ::Int64 with non-default value
SELECT 'bloom_filter CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b::Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1e: CAST ::Int64 = 0 — unsafe (0 is default for Int64), index not used
SELECT 'bloom_filter CAST default value unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b::Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1f: Typed subcolumn .:Int64 = 0 — safe (Nullable, NULL for missing path)
SELECT 'bloom_filter typed subcolumn zero safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b.:Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1g: Reversed argument order
SELECT 'bloom_filter reversed equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE 1 = json.a.b)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1h: Reversed + CAST
SELECT 'bloom_filter reversed CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE 1 = json.a.b::Int64)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 2: IN
-- =============================================================================

-- 2a: IN with typed subcolumn (Nullable)
SELECT 'bloom_filter IN typed';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b.:Int64 IN (1, 2, 3))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2b: IN with CAST (no default in set)
SELECT 'bloom_filter IN CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b::Int64 IN (1, 2, 3))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2c: IN with CAST (default in set) — unsafe
SELECT 'bloom_filter IN CAST default in set unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b::Int64 IN (0, 1, 2))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2d: IN with Nullable (default in set) — safe because Nullable
SELECT 'bloom_filter IN Nullable default in set safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b.:Int64 IN (0, 1, 2))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 3: IS NOT NULL
-- =============================================================================

-- 3a: IS NOT NULL on Dynamic subcolumn — path a.b only in part 1
SELECT 'bloom_filter IS NOT NULL Dynamic';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3b: IS NOT NULL on typed subcolumn (Nullable)
SELECT 'bloom_filter IS NOT NULL typed';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.a.b.:Int64 IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3c: IS NOT NULL on non-existing path — skip all
SELECT 'bloom_filter IS NOT NULL nonexistent';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE json.nonexistent IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3d: IS NOT NULL with CAST to non-Nullable type — unsafe, index should NOT be used
--     CAST(NULL, 'Int64') → 0, isNotNull(0) = true → skipping would be incorrect
SELECT 'bloom_filter IS NOT NULL CAST unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_bf WHERE isNotNull(json.a.b::Int64))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_bf;

-- =============================================================================
-- Section 4: Safety semantics — CAST to String, Nullable, Float64
-- =============================================================================

DROP TABLE IF EXISTS t_json_safety;

CREATE TABLE t_json_safety
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_safety VALUES (1, '{"a": "hello"}'), (2, '{"b": "world"}');
INSERT INTO t_json_safety VALUES (3, '{"c": "test"}'), (4, '{"d": "extra"}');

-- 4a: CAST ::String = '' — unsafe ('' is default for String)
SELECT 'CAST String empty unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_safety WHERE json.a::String = '')
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 4b: CAST ::String = 'hello' — safe
SELECT 'CAST String non-empty safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_safety WHERE json.a::String = 'hello')
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 4c: CAST to Nullable(Int64) = 0 — safe (Nullable)
SELECT 'CAST Nullable safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_safety WHERE CAST(json.a, 'Nullable(Int64)') = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 4d: CAST ::Float64 = 0.0 — unsafe (0.0 is default for Float64)
SELECT 'CAST Float64 default unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_safety WHERE json.a::Float64 = 0.0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 4e: CAST ::Float64 = 1.5 — safe
SELECT 'CAST Float64 non-default safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_safety WHERE json.a::Float64 = 1.5)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_safety;

-- =============================================================================
-- Section 5: Array type subcolumn
-- =============================================================================

DROP TABLE IF EXISTS t_json_arr;
CREATE TABLE t_json_arr
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_arr VALUES (1, '{"a": [1, 2, 3]}'), (2, '{"b": [4, 5]}');
INSERT INTO t_json_arr VALUES (3, '{"c": [6]}'), (4, '{"d": [7, 8]}');

-- 5a: Array subcolumn = [] — unsafe
SELECT 'Array subcolumn empty unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_arr WHERE json.a.:`Array(Nullable(Int64))` = [])
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 5b: Array subcolumn = [1,2,3] — safe
SELECT 'Array subcolumn non-empty safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_arr WHERE json.a.:`Array(Nullable(Int64))` = [1, 2, 3])
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_arr;

-- =============================================================================
-- Section 6: Sub-object ^ access
-- =============================================================================

DROP TABLE IF EXISTS t_json_subobj;
CREATE TABLE t_json_subobj
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_subobj VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_subobj VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- 6a: Sub-object access — index should NOT be used
SELECT 'sub-object ^ access not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_subobj WHERE empty(json.^a))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_subobj;

-- =============================================================================
-- Section 7: AND/OR with multiple paths
-- =============================================================================

DROP TABLE IF EXISTS t_json_multi;
CREATE TABLE t_json_multi
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_multi VALUES (1, '{"a": 1, "b": 2}'), (2, '{"a": 3, "c": 4}');
INSERT INTO t_json_multi VALUES (3, '{"d": 5, "e": 6}'), (4, '{"f": 7, "g": 8}');

-- 7a: AND — both paths in part 1, granule 1
SELECT 'AND both paths';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_multi WHERE json.a = 1 AND json.b = 2)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 7b: AND — one path non-existing
SELECT 'AND one non-existing';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_multi WHERE json.a = 1 AND json.nonexistent = 99)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 7c: OR — path a in part 1, path d in part 2
SELECT 'OR different paths';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_multi WHERE json.a = 1 OR json.d = 5)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_multi;

-- =============================================================================
-- Section 8: Dotted column name and Tuple(JSON)
-- =============================================================================

DROP TABLE IF EXISTS t_json_dotted;
CREATE TABLE t_json_dotted
(
    id UInt64,
    `my.json` JSON,
    INDEX idx JSONAllPaths(`my.json`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_dotted VALUES (1, '{"path": 1}'), (2, '{"other": 2}');
INSERT INTO t_json_dotted VALUES (3, '{"third": 3}'), (4, '{"fourth": 4}');

SELECT 'dotted column name';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_dotted WHERE `my.json`.path = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_dotted;

DROP TABLE IF EXISTS t_json_tuple;
CREATE TABLE t_json_tuple
(
    id UInt64,
    t Tuple(json JSON),
    INDEX idx JSONAllPaths(t.json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_tuple VALUES (1, tuple('{"a": 1}')), (2, tuple('{"b": 2}'));
INSERT INTO t_json_tuple VALUES (3, tuple('{"c": 3}')), (4, tuple('{"d": 4}'));

SELECT 'Tuple(JSON) subcolumn';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tuple WHERE t.json.a = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_tuple;

-- =============================================================================
-- Section 9: Correctness
-- =============================================================================

DROP TABLE IF EXISTS t_json_results;
CREATE TABLE t_json_results
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_results VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_results VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- Single-path correctness
SELECT 'result: equals';
SELECT id FROM t_json_results WHERE json.a.b = 1 ORDER BY id;

SELECT 'result: typed subcolumn';
SELECT id FROM t_json_results WHERE json.a.b.:Int64 = 1 ORDER BY id;

SELECT 'result: CAST';
SELECT id FROM t_json_results WHERE json.a.b::Int64 = 1 ORDER BY id;

SELECT 'result: IN';
SELECT id FROM t_json_results WHERE json.a.b.:Int64 IN (1, 2) ORDER BY id;

SELECT 'result: non-existing';
SELECT id FROM t_json_results WHERE json.nonexistent = 1 ORDER BY id;

SELECT 'result: reversed';
SELECT id FROM t_json_results WHERE 1 = json.a.b ORDER BY id;

SELECT 'result: CAST default returns all rows with path absent';
SELECT id FROM t_json_results WHERE json.a.b::Int64 = 0 ORDER BY id;

SELECT 'result: NOT IN CAST';
SELECT id FROM t_json_results WHERE json.a.b::Int64 NOT IN (1) ORDER BY id;

-- Multi-path correctness
SELECT 'result: AND same row';
SELECT id FROM t_json_results WHERE json.a.b = 1 AND json.c = 'hello' ORDER BY id;

SELECT 'result: OR across parts';
SELECT id FROM t_json_results WHERE json.a.b = 1 OR json.x.y = 3 ORDER BY id;

SELECT 'result: AND nonexistent';
SELECT id FROM t_json_results WHERE json.a.b = 1 AND json.nonexistent = 99 ORDER BY id;

SELECT 'result: typed AND CAST';
SELECT id FROM t_json_results WHERE json.a.b.:Int64 = 1 AND json.c::String = 'hello' ORDER BY id;

SELECT 'result: CAST OR CAST';
SELECT id FROM t_json_results WHERE json.a.b::Int64 = 1 OR json.e::String = 'world' ORDER BY id;

SELECT 'result: typed AND Dynamic';
SELECT id FROM t_json_results WHERE json.a.d.:Int64 = 2 AND json.e = 'world' ORDER BY id;

SELECT 'result: triple OR';
SELECT id FROM t_json_results WHERE json.a.b = 1 OR json.a.d = 2 OR json.x.y = 3 ORDER BY id;

-- IS NOT NULL correctness
SELECT 'result: IS NOT NULL';
SELECT id FROM t_json_results WHERE json.a.b IS NOT NULL ORDER BY id;

SELECT 'result: IS NOT NULL nonexistent';
SELECT id FROM t_json_results WHERE json.nonexistent IS NOT NULL ORDER BY id;

SELECT 'result: IS NOT NULL AND equals';
SELECT id FROM t_json_results WHERE json.a.b IS NOT NULL AND json.c = 'hello' ORDER BY id;

DROP TABLE t_json_results;
