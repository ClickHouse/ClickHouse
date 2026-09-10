-- This is a smoke test, non exhaustive.

-- Conversions

SELECT
    1::BFloat16,
    -1::BFloat16,
    1.1::BFloat16,
    -1.1::BFloat16,
    CAST(1 AS BFloat16),
    CAST(-1 AS BFloat16),
    CAST(1.1 AS BFloat16),
    CAST(-1.1 AS BFloat16),
    CAST(0xFFFFFFFFFFFFFFFF AS BFloat16),
    CAST(-0.0 AS BFloat16),
    CAST(inf AS BFloat16),
    CAST(-inf AS BFloat16),
    CAST(nan AS BFloat16);

-- Conversions back

SELECT
    CAST(1.1::BFloat16 AS BFloat16),
    CAST(1.1::BFloat16 AS Float32),
    CAST(1.1::BFloat16 AS Float64),
    CAST(1.1::BFloat16 AS Int8);

-- Comparisons

SELECT
    1.1::BFloat16 = 1.1::BFloat16,
    1.1::BFloat16 < 1.1,
    1.1::BFloat16 > 1.1,
    1.1::BFloat16 > 1,
    1.1::BFloat16 = 1.09375;

-- Arithmetic

SELECT
    1.1::BFloat16 - 1.1::BFloat16 AS a,
    1.1::BFloat16 + 1.1::BFloat16 AS b,
    1.1::BFloat16 * 1.1::BFloat16 AS c,
    1.1::BFloat16 / 1.1::BFloat16 AS d,
    toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d);

SELECT
    1.1::BFloat16 - 1.1 AS a,
    1.1 + 1.1::BFloat16 AS b,
    1.1::BFloat16 * 1.1 AS c,
    1.1 / 1.1::BFloat16 AS d,
    toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d);

-- Tables

DROP TABLE IF EXISTS t;
CREATE TEMPORARY TABLE t (n UInt64, x BFloat16);
INSERT INTO t SELECT number, number FROM numbers(10000);
SELECT *, n = x, n - x FROM t WHERE n % 1000 = 0 ORDER BY n;

-- Aggregate functions

SELECT sum(n), sum(x), avg(n), avg(x), min(n), min(x), max(n), max(x), uniq(n), uniq(x), uniqExact(n), uniqExact(x) FROM t;

-- MergeTree

DROP TABLE t;
CREATE TABLE t (n UInt64, x BFloat16) ENGINE = MergeTree ORDER BY n;
INSERT INTO t SELECT number, number FROM numbers(10000);
SELECT *, n = x, n - x FROM t WHERE n % 1000 = 0 ORDER BY n;
SELECT sum(n), sum(x), avg(n), avg(x), min(n), min(x), max(n), max(x), uniq(n), uniq(x), uniqExact(n), uniqExact(x) FROM t;

-- Distances

WITH
    arrayMap(x -> toFloat32(x) / 2, range(384)) AS a32,
    arrayMap(x -> toBFloat16(x) / 2, range(384)) AS a16,
    arrayMap(x -> x + 1, a32) AS a32_1,
    arrayMap(x -> x + 1, a16) AS a16_1
SELECT a32, a16, a32_1, a16_1,
    dotProduct(a32, a32_1), dotProduct(a16, a16_1),
    cosineDistance(a32, a32_1), cosineDistance(a16, a16_1),
    L2Distance(a32, a32_1), L2Distance(a16, a16_1),
    L1Distance(a32, a32_1), L1Distance(a16, a16_1),
    LinfDistance(a32, a32_1), LinfDistance(a16, a16_1),
    LpDistance(a32, a32_1, 5), LpDistance(a16, a16_1, 5)
FORMAT Vertical;

-- Introspection

SELECT 1.1::BFloat16 AS x,
    hex(x), bin(x),
    byteSize(x),
    reinterpretAsUInt16(x), hex(reinterpretAsString(x));

-- Rounding (this could be not towards the nearest)

SELECT 1.1::BFloat16 AS x,
    round(x), round(x, 1), round(x, 2), round(x, -1);
