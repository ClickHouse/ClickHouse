-- Test: Verify correctness of min/max/argMin/argMax with -If combinators for Decimal32/64 and DateTime64
-- Covers: src/Common/findExtreme.cpp:160-171 (optimized SIMD path for Decimal types via reinterpret_cast)
-- These types now use the findExtreme* optimized path; no existing test covers -If/-NotNull combinators.

SET session_timezone = 'UTC';

-- Use small block size to exercise the cross-block merge path in SingleValueData
SET max_threads = 1, max_block_size = 10;

-- Decimal32 minIf/maxIf
SELECT 'Decimal32 minIf/maxIf';
SELECT
    minIf(d, d > toDecimal32(0, 2)),
    maxIf(d, d < toDecimal32(50, 2))
FROM (SELECT toDecimal32(number - 50, 2) AS d FROM numbers(101))
ORDER BY 1;

-- Decimal64 minIf/maxIf
SELECT 'Decimal64 minIf/maxIf';
SELECT
    minIf(d, d > toDecimal64(0, 6)),
    maxIf(d, d < toDecimal64(50, 6))
FROM (SELECT toDecimal64(number - 50, 6) AS d FROM numbers(101))
ORDER BY 1;

-- DateTime64 minIf/maxIf
SELECT 'DateTime64 minIf/maxIf';
SELECT
    minIf(d, d > toDateTime64('1970-01-01 00:00:10', 6)),
    maxIf(d, d < toDateTime64('1970-01-01 00:00:50', 6))
FROM (SELECT toDateTime64(number, 6) AS d FROM numbers(100))
ORDER BY 1;

-- Nullable Decimal32 min/max (exercises setSmallestNotNullIf)
SELECT 'Nullable Decimal32 min/max';
SELECT min(d), max(d)
FROM (SELECT if(number % 3 = 0, NULL, toDecimal32(number - 50, 2)) AS d FROM numbers(101))
ORDER BY 1;

-- Nullable Decimal64 min/max
SELECT 'Nullable Decimal64 min/max';
SELECT min(d), max(d)
FROM (SELECT if(number % 3 = 0, NULL, toDecimal64(number - 50, 6)) AS d FROM numbers(101))
ORDER BY 1;

-- Nullable DateTime64 min/max
SELECT 'Nullable DateTime64 min/max';
SELECT min(d), max(d)
FROM (SELECT if(number % 3 = 0, NULL, toDateTime64(number, 6)) AS d FROM numbers(100))
ORDER BY 1;

-- argMinIf/argMaxIf with Decimal32 (exercises getSmallestIndexNotNullIf)
SELECT 'Decimal32 argMinIf/argMaxIf';
SELECT
    argMinIf(x, d, d > toDecimal32(0, 2)),
    argMaxIf(x, d, d < toDecimal32(50, 2))
FROM (SELECT number AS x, toDecimal32(number - 50, 2) AS d FROM numbers(101))
ORDER BY 1;

-- argMinIf/argMaxIf with Decimal64
SELECT 'Decimal64 argMinIf/argMaxIf';
SELECT
    argMinIf(x, d, d > toDecimal64(0, 6)),
    argMaxIf(x, d, d < toDecimal64(50, 6))
FROM (SELECT number AS x, toDecimal64(number - 50, 6) AS d FROM numbers(101))
ORDER BY 1;

-- argMinIf/argMaxIf with DateTime64
SELECT 'DateTime64 argMinIf/argMaxIf';
SELECT
    argMinIf(x, d, d > toDateTime64('1970-01-01 00:00:10', 6)),
    argMaxIf(x, d, d < toDateTime64('1970-01-01 00:00:50', 6))
FROM (SELECT number AS x, toDateTime64(number, 6) AS d FROM numbers(100))
ORDER BY 1;

-- argMin/argMax with Nullable Decimal32 (exercises getSmallestIndexNotNullIf with null_map)
SELECT 'Nullable Decimal32 argMin/argMax';
SELECT
    argMin(x, d),
    argMax(x, d)
FROM (SELECT number AS x, if(number % 3 = 0, NULL, toDecimal32(number - 50, 2)) AS d FROM numbers(101))
ORDER BY 1;
