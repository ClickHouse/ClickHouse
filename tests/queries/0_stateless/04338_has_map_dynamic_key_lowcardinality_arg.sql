-- 'has' over a Map with a Dynamic key and a LowCardinality lookup argument used to
-- crash (SIGSEGV in release / type-mismatch assertion in debug) because executeMap()
-- reached executeGeneric() without stripping LowCardinality from the lookup column.

SET allow_experimental_dynamic_type = 1;

SELECT has(map('a'::Dynamic, toLowCardinality('x')), toLowCardinality('b'));
SELECT has(map('a'::Dynamic, toLowCardinality('x'), 'b'::Dynamic, toLowCardinality('y')), toLowCardinality('b'));
SELECT has(map(_CAST('1000.0001', 'Dynamic(max_types=19)'), toLowCardinality(toString(0))), toLowCardinality(';--'));
SELECT has(map('a'::Dynamic, 1), 'a');

DROP TABLE IF EXISTS t_04338;
CREATE TABLE t_04338 (m Map(Dynamic, String), k LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_04338 VALUES (map('x'::Dynamic, 'p'), 'x'), (map('y'::Dynamic, 'q'), 'z');
SELECT k, has(m, k) FROM t_04338 ORDER BY k;
DROP TABLE t_04338;

SET allow_suspicious_low_cardinality_types = 1;

-- LowCardinality(Nullable(T)) needle: the null map must be honoured on the Map path.
-- has(m, k) must agree with the plain array path has(mapKeys(m), k) for every row.
SELECT has(map(NULL::Dynamic, 1), CAST(NULL, 'LowCardinality(Nullable(String))'));
SELECT has(mapKeys(map(NULL::Dynamic, 1)), CAST(NULL, 'LowCardinality(Nullable(String))'));

DROP TABLE IF EXISTS t_04338_nullable;
CREATE TABLE t_04338_nullable (m Map(Dynamic, UInt8), k LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO t_04338_nullable VALUES
    (map('a'::Dynamic, 1), 'a'),
    (map('a'::Dynamic, 1), 'b'),
    (map('a'::Dynamic, 1), NULL),
    (map(NULL::Dynamic, 1), NULL),
    (map(NULL::Dynamic, 1), 'a');
SELECT k, has(m, k) AS map_path, has(mapKeys(m), k) AS array_path
FROM t_04338_nullable ORDER BY k, map_path;
DROP TABLE t_04338_nullable;
