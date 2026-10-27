-- Tags: no-random-settings, no-fasttest
-- Regression guard for window-partition scatter memory overhead.
-- This should pass with the fix and fail on unpatched binary under the same memory limit.

-- Avoid storage overhead in flaky checks and force tiny source blocks.
SET max_block_size = 8;
SET max_threads = 8, max_memory_usage = 80000000;

SELECT '--- explain pipeline ---';
SELECT toUInt8(count() > 0)
FROM
(
    EXPLAIN PIPELINE
    SELECT
        LEAST(F016 - F040, F021) AS AQ,
        F028 * IF(F026 = 'some', 1, -1) AS SBP,
        SUM(AQ) OVER (PARTITION BY F047, F022, F001, F046 ORDER BY SBP ASC, F025 ASC) AS CSF
    FROM
    (
        SELECT
            CAST(NULL, 'Nullable(String)') AS F001,
            CAST(NULL, 'Nullable(UInt64)') AS F022,
            CAST(NULL, 'Nullable(String)') AS F046,
            CAST(NULL, 'Nullable(String)') AS F047,
            toNullable(toInt64(number % 100000)) AS F016,
            toNullable(toInt64(number % 1000)) AS F040,
            toNullable(toInt64(number % 50000)) AS F021,
            toNullable(toFloat64(number % 1000) / 10) AS F028,
            toNullable(if(number % 2 = 0, 'some', 'other')) AS F026,
            toNullable(toString(number % 100000)) AS F025
        FROM system.numbers_mt
        LIMIT 100000
    )
    LIMIT 1
)
WHERE explain LIKE '%ScatterByPartitionTransform%';

SELECT '--- query execution ---';
SELECT
    LEAST(F016 - F040, F021) AS AQ,
    F028 * IF(F026 = 'some', 1, -1) AS SBP,
    SUM(AQ) OVER (PARTITION BY F047, F022, F001, F046 ORDER BY SBP ASC, F025 ASC) AS CSF
FROM
(
    SELECT
        CAST(NULL, 'Nullable(String)') AS F001,
        CAST(NULL, 'Nullable(UInt64)') AS F022,
        CAST(NULL, 'Nullable(String)') AS F046,
        CAST(NULL, 'Nullable(String)') AS F047,
        toNullable(toInt64(number % 100000)) AS F016,
        toNullable(toInt64(number % 1000)) AS F040,
        toNullable(toInt64(number % 50000)) AS F021,
        toNullable(toFloat64(number % 1000) / 10) AS F028,
        toNullable(if(number % 2 = 0, 'some', 'other')) AS F026,
        toNullable(toString(number % 100000)) AS F025
    FROM system.numbers_mt
    LIMIT 100000
)
LIMIT 1
FORMAT Null;

SELECT 'OK';
