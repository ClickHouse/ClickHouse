-- Tags: no-fasttest, no-parallel-replicas
-- Test: tokenbf_v1 and ngrambf_v1 skip index support for JSONAllPaths on JSON data type
--
-- Data layout: 2 parts x 2 granules each (index_granularity = 1, ORDER BY tuple()).
--   Part 1: (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}')
--   Part 2: (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}')

-- =============================================================================
-- Section 1: tokenbf_v1 — Equals
-- =============================================================================

DROP TABLE IF EXISTS t_json_tbf;
CREATE TABLE t_json_tbf
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_tbf VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_tbf VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- 1a: Dynamic equals
SELECT 'tokenbf equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1b: Non-existing path
SELECT 'tokenbf non-existing';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1c: CAST equals
SELECT 'tokenbf CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b::Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1d: CAST default value — unsafe
SELECT 'tokenbf CAST default unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b::Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1e: Typed subcolumn zero — safe (Nullable)
SELECT 'tokenbf typed zero safe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b.:Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1f: Reversed argument order
SELECT 'tokenbf reversed equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE 1 = json.a.b)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 2: tokenbf_v1 — IS NOT NULL
-- =============================================================================

-- 2a: IS NOT NULL on Dynamic subcolumn — path a.b only in part 1
SELECT 'tokenbf IS NOT NULL Dynamic';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2b: IS NOT NULL on typed subcolumn (Nullable)
SELECT 'tokenbf IS NOT NULL typed';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.a.b.:Int64 IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2c: IS NOT NULL on non-existing path — skip all
SELECT 'tokenbf IS NOT NULL nonexistent';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE json.nonexistent IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2d: IS NOT NULL with CAST to non-Nullable type — unsafe, index should NOT be used
SELECT 'tokenbf IS NOT NULL CAST unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE isNotNull(json.a.b::Int64))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- =============================================================================
-- Section 3: tokenbf_v1 — Sub-object ^ access
-- =============================================================================

-- 3a: Sub-object access — index should NOT be used
SELECT 'tokenbf sub-object ^ not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_tbf WHERE empty(json.^a))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_tbf;

-- =============================================================================
-- Section 6: ngrambf_v1
-- =============================================================================

DROP TABLE IF EXISTS t_json_ngram;
CREATE TABLE t_json_ngram
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_ngram VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_ngram VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- 6a: Equals
SELECT 'ngrambf equals';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_ngram WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 6b: Non-existing
SELECT 'ngrambf non-existing';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_ngram WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 6c: CAST equals
SELECT 'ngrambf CAST';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_ngram WHERE json.a.b::Int64 = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 6d: CAST default — unsafe
SELECT 'ngrambf CAST default unsafe';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_ngram WHERE json.a.b::Int64 = 0)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 6e: IS NOT NULL
SELECT 'ngrambf IS NOT NULL';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_ngram WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_ngram;

-- =============================================================================
-- Section 7: Correctness
-- =============================================================================

DROP TABLE IF EXISTS t_json_tbf_res;
CREATE TABLE t_json_tbf_res
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_tbf_res VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_tbf_res VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

-- Single-path
SELECT 'result: tokenbf equals';
SELECT id FROM t_json_tbf_res WHERE json.a.b = 1 ORDER BY id;

SELECT 'result: tokenbf non-existing';
SELECT id FROM t_json_tbf_res WHERE json.nonexistent = 1 ORDER BY id;

-- Multi-path
SELECT 'result: tokenbf AND';
SELECT id FROM t_json_tbf_res WHERE json.a.b = 1 AND json.c = 'hello' ORDER BY id;

SELECT 'result: tokenbf OR';
SELECT id FROM t_json_tbf_res WHERE json.a.b = 1 OR json.x.y = 3 ORDER BY id;

SELECT 'result: tokenbf AND nonexistent';
SELECT id FROM t_json_tbf_res WHERE json.a.b = 1 AND json.nonexistent = 99 ORDER BY id;

-- IS NOT NULL correctness
SELECT 'result: tokenbf IS NOT NULL';
SELECT id FROM t_json_tbf_res WHERE json.a.b IS NOT NULL ORDER BY id;

SELECT 'result: tokenbf IS NOT NULL nonexistent';
SELECT id FROM t_json_tbf_res WHERE json.nonexistent IS NOT NULL ORDER BY id;

DROP TABLE t_json_tbf_res;

DROP TABLE IF EXISTS t_json_ngram_res;
CREATE TABLE t_json_ngram_res
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_json_ngram_res VALUES (1, '{"a": {"b": 1}, "c": "hello"}'), (2, '{"a": {"d": 2}, "e": "world"}');
INSERT INTO t_json_ngram_res VALUES (3, '{"x": {"y": 3}, "z": "test"}'), (4, '{"p": {"q": 4}, "r": "foo"}');

SELECT 'result: ngrambf equals';
SELECT id FROM t_json_ngram_res WHERE json.a.b = 1 ORDER BY id;

DROP TABLE t_json_ngram_res;
