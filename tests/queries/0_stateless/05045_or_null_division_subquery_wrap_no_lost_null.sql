-- Division by a constant zero must return NULL from the *OrNull functions no matter which projection consumes the value.
-- See https://github.com/ClickHouse/ClickHouse/issues/116500

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS sink;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS ctas;

CREATE TABLE test (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (1), (2), (3), (4);

-- The reported shape: a passthrough subquery around a constant-folding dividend.
SELECT 'wrapped', m FROM (SELECT moduloOrNull(byteSize(x), 0) AS m FROM test);

-- The same expressions evaluated without an outer projection.
SELECT 'direct', moduloOrNull(byteSize(x), 0), intDivOrNull(byteSize(x), 0), positiveModuloOrNull(byteSize(x), 0), divideOrNull(byteSize(x), 0) FROM test;

-- All four functions of the family, consumed through an outer projection.
SELECT 'family', count(), countIf(m1 IS NULL), countIf(m2 IS NULL), countIf(m3 IS NULL), countIf(m4 IS NULL)
FROM (SELECT moduloOrNull(byteSize(x), 0) AS m1, intDivOrNull(byteSize(x), 0) AS m2,
             positiveModuloOrNull(byteSize(x), 0) AS m3, divideOrNull(byteSize(x), 0) AS m4 FROM test);

-- Float, wide and Nullable divisors reach different type dispatch paths.
SELECT 'divisor types', count(), countIf(f IS NULL), countIf(w IS NULL), countIf(n IS NULL)
FROM (SELECT moduloOrNull(byteSize(x), 0.) AS f, moduloOrNull(byteSize(x), toInt128(0)) AS w,
             moduloOrNull(byteSize(x), toNullable(0)) AS n FROM test);

SELECT 'lowcardinality', count(), countIf(m IS NULL) FROM (SELECT moduloOrNull(byteSize(x), toLowCardinality(0)) AS m FROM test);
SELECT 'lowcardinality nullable', count(), countIf(m IS NULL) FROM (SELECT moduloOrNull(byteSize(x), toLowCardinality(toNullable(0))) AS m FROM test);

SELECT 'always null divisor', count(), countIf(m IS NULL) FROM (SELECT moduloOrNull(byteSize(x), CAST(NULL, 'Nullable(UInt8)')) AS m FROM test);

-- A stored result keeps whatever the projection produced.
CREATE TABLE sink (m Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO sink SELECT * FROM (SELECT moduloOrNull(byteSize(x), 0) AS m FROM test);
SELECT 'stored', count(), countIf(m IS NULL) FROM sink;

-- Selecting from a view is itself a wrap, so a view needs no explicit subquery.
CREATE VIEW v AS SELECT moduloOrNull(byteSize(x), 0) AS m FROM test;
SELECT 'view', count(), countIf(m IS NULL) FROM v;
CREATE TABLE ctas ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM (SELECT moduloOrNull(byteSize(x), 0) AS m FROM test);
SELECT 'ctas', count(), countIf(m IS NULL) FROM ctas;

-- A dividend or divisor that stays non-constant never takes the constant path.
SELECT 'non-const dividend', count(), countIf(a IS NULL), countIf(b IS NULL)
FROM (SELECT moduloOrNull(x, 0) AS a, moduloOrNull(materialize(x), 0) AS b FROM test);
SELECT 'non-const divisor', count(), countIf(m IS NULL) FROM (SELECT moduloOrNull(byteSize(x), x - x) AS m FROM test);

-- intDivOrZero is outside the family and keeps returning zero.
SELECT 'or zero', count(), countIf(m IS NULL), sum(m) FROM (SELECT intDivOrZero(byteSize(x), 0) AS m FROM test);

-- A divisor that does not raise keeps its value.
SELECT 'nonzero divisor', count(), countIf(m IS NULL), sum(m) FROM (SELECT moduloOrNull(byteSize(x), 3) AS m FROM test);
SELECT 'row values', x, moduloOrNull(x, 3) FROM test ORDER BY x;

SELECT 'type', toTypeName(m) FROM (SELECT * FROM (SELECT moduloOrNull(byteSize(x), 0) AS m FROM test)) LIMIT 1;

DROP VIEW v;
DROP TABLE ctas;
DROP TABLE sink;
DROP TABLE test;
