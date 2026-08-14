-- Tags: no-fasttest, no-parallel-replicas
-- Test: JSON skip index with larger granularity and multi-row granules
--
-- Data layout: 2 parts x 2 granules each (index_granularity = 3, ORDER BY tuple()).
--   Part 1 (6 rows, 2 granules):
--     Granule 1: rows 1-3 (paths: a.b, c, a.d, e, a.f)
--     Granule 2: rows 4-6 (paths: x.y, z, x.w, m, n)
--   Part 2 (6 rows, 2 granules):
--     Granule 3: rows 7-9  (paths: p.q, r, p.s, t, u)
--     Granule 4: rows 10-12 (paths: v.k, g, v.l, h, j)

-- =============================================================================
-- Section 1: bloom_filter with index_granularity = 3
-- =============================================================================

DROP TABLE IF EXISTS t_json_large_bf;
CREATE TABLE t_json_large_bf
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 3;

-- Part 1: 6 rows → 2 granules
INSERT INTO t_json_large_bf VALUES
    (1, '{"a": {"b": 1}, "c": "hello"}'),
    (2, '{"a": {"d": 2}, "e": "world"}'),
    (3, '{"a": {"f": 3}}'),
    (4, '{"x": {"y": 10}, "z": "foo"}'),
    (5, '{"x": {"w": 20}, "m": "bar"}'),
    (6, '{"n": 30}');

-- Part 2: 6 rows → 2 granules
INSERT INTO t_json_large_bf VALUES
    (7, '{"p": {"q": 100}, "r": "baz"}'),
    (8, '{"p": {"s": 200}, "t": "qux"}'),
    (9, '{"u": 300}'),
    (10, '{"v": {"k": 400}, "g": "aaa"}'),
    (11, '{"v": {"l": 500}, "h": "bbb"}'),
    (12, '{"j": 600}');

-- 1a: Path a.b only in part 1, granule 1
SELECT 'bf large: a.b = 1';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1b: Path x.y only in part 1, granule 2
SELECT 'bf large: x.y = 10';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.x.y = 10)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1c: Path p.q only in part 2, granule 3
SELECT 'bf large: p.q = 100';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.p.q = 100)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1d: Non-existing path — skip all parts and granules
SELECT 'bf large: nonexistent';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1e: OR — paths in part 1 granule 1 and part 2 granule 3
SELECT 'bf large: OR across parts';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b = 1 OR json.p.q = 100)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1f: AND — both paths in part 1 granule 1
SELECT 'bf large: AND same granule';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b = 1 AND json.c = 'hello')
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1g: AND — paths in different parts → 0
SELECT 'bf large: AND different parts';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b = 1 AND json.p.q = 100)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1h: OR — paths in same part different granules
SELECT 'bf large: OR same part different granules';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b = 1 OR json.x.y = 10)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 1i: IS NOT NULL — path a.b in part 1 granule 1
SELECT 'bf large: IS NOT NULL';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_bf WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_large_bf;

-- =============================================================================
-- Section 2: tokenbf_v1 with index_granularity = 3
-- =============================================================================

DROP TABLE IF EXISTS t_json_large_tbf;
CREATE TABLE t_json_large_tbf
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 3;

INSERT INTO t_json_large_tbf VALUES
    (1, '{"a": {"b": 1}, "c": "hello"}'),
    (2, '{"a": {"d": 2}, "e": "world"}'),
    (3, '{"a": {"f": 3}}'),
    (4, '{"x": {"y": 10}, "z": "foo"}'),
    (5, '{"x": {"w": 20}, "m": "bar"}'),
    (6, '{"n": 30}');

INSERT INTO t_json_large_tbf VALUES
    (7, '{"p": {"q": 100}, "r": "baz"}'),
    (8, '{"p": {"s": 200}, "t": "qux"}'),
    (9, '{"u": 300}'),
    (10, '{"v": {"k": 400}, "g": "aaa"}'),
    (11, '{"v": {"l": 500}, "h": "bbb"}'),
    (12, '{"j": 600}');

-- 2a: Path a.b only in part 1, granule 1
SELECT 'tbf large: a.b = 1';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_tbf WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2b: Non-existing
SELECT 'tbf large: nonexistent';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_tbf WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2c: OR — paths in part 1 and part 2
SELECT 'tbf large: OR across parts';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_tbf WHERE json.a.b = 1 OR json.p.q = 100)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 2d: IS NOT NULL
SELECT 'tbf large: IS NOT NULL';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_tbf WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_large_tbf;

-- =============================================================================
-- Section 3: text index with index_granularity = 3
-- =============================================================================

DROP TABLE IF EXISTS t_json_large_text;
CREATE TABLE t_json_large_text
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 3;

INSERT INTO t_json_large_text VALUES
    (1, '{"a": {"b": 1}, "c": "hello"}'),
    (2, '{"a": {"d": 2}, "e": "world"}'),
    (3, '{"a": {"f": 3}}'),
    (4, '{"x": {"y": 10}, "z": "foo"}'),
    (5, '{"x": {"w": 20}, "m": "bar"}'),
    (6, '{"n": 30}');

INSERT INTO t_json_large_text VALUES
    (7, '{"p": {"q": 100}, "r": "baz"}'),
    (8, '{"p": {"s": 200}, "t": "qux"}'),
    (9, '{"u": 300}'),
    (10, '{"v": {"k": 400}, "g": "aaa"}'),
    (11, '{"v": {"l": 500}, "h": "bbb"}'),
    (12, '{"j": 600}');

-- 3a: Path a.b only in part 1, granule 1
SELECT 'text large: a.b = 1';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_text WHERE json.a.b = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3b: Non-existing
SELECT 'text large: nonexistent';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_text WHERE json.nonexistent = 1)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3c: OR — paths in part 1 and part 2
SELECT 'text large: OR across parts';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_text WHERE json.a.b = 1 OR json.p.q = 100)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

-- 3d: IS NOT NULL
SELECT 'text large: IS NOT NULL';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_large_text WHERE json.a.b IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_large_text;

-- =============================================================================
-- Section 4: Correctness with larger dataset (bloom_filter)
-- =============================================================================

DROP TABLE IF EXISTS t_json_large_res;
CREATE TABLE t_json_large_res
(
    id UInt64,
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 3;

INSERT INTO t_json_large_res VALUES
    (1, '{"a": {"b": 1}, "c": "hello"}'),
    (2, '{"a": {"d": 2}, "e": "world"}'),
    (3, '{"a": {"f": 3}}'),
    (4, '{"x": {"y": 10}, "z": "foo"}'),
    (5, '{"x": {"w": 20}, "m": "bar"}'),
    (6, '{"n": 30}');

INSERT INTO t_json_large_res VALUES
    (7, '{"p": {"q": 100}, "r": "baz"}'),
    (8, '{"p": {"s": 200}, "t": "qux"}'),
    (9, '{"u": 300}'),
    (10, '{"v": {"k": 400}, "g": "aaa"}'),
    (11, '{"v": {"l": 500}, "h": "bbb"}'),
    (12, '{"j": 600}');

-- Single-path
SELECT 'large result: a.b = 1';
SELECT id FROM t_json_large_res WHERE json.a.b = 1 ORDER BY id;

SELECT 'large result: x.y = 10';
SELECT id FROM t_json_large_res WHERE json.x.y = 10 ORDER BY id;

SELECT 'large result: nonexistent';
SELECT id FROM t_json_large_res WHERE json.nonexistent = 1 ORDER BY id;

-- Multi-path
SELECT 'large result: AND same granule';
SELECT id FROM t_json_large_res WHERE json.a.b = 1 AND json.c = 'hello' ORDER BY id;

SELECT 'large result: OR across parts';
SELECT id FROM t_json_large_res WHERE json.a.b = 1 OR json.p.q = 100 ORDER BY id;

SELECT 'large result: AND different parts';
SELECT id FROM t_json_large_res WHERE json.a.b = 1 AND json.p.q = 100 ORDER BY id;

SELECT 'large result: typed AND Dynamic';
SELECT id FROM t_json_large_res WHERE json.a.d.:Int64 = 2 AND json.e = 'world' ORDER BY id;

SELECT 'large result: OR different leaves';
SELECT id FROM t_json_large_res WHERE json.n = 30 OR json.u = 300 ORDER BY id;

-- IS NOT NULL correctness
SELECT 'large result: IS NOT NULL a.b';
SELECT id FROM t_json_large_res WHERE json.a.b IS NOT NULL ORDER BY id;

SELECT 'large result: IS NOT NULL nonexistent';
SELECT id FROM t_json_large_res WHERE json.nonexistent IS NOT NULL ORDER BY id;

DROP TABLE t_json_large_res;
