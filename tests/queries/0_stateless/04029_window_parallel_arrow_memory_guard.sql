-- Tags: no-random-settings
-- Regression guard for window-partition scatter memory overhead.
-- This should pass with the fix and fail on unpatched binary under the same memory limit.

DROP TABLE IF EXISTS window_parallel_arrow_memory_guard;

CREATE TABLE window_parallel_arrow_memory_guard
(
    F001 Nullable(String),
    F022 Nullable(UInt64),
    F046 Nullable(String),
    F047 Nullable(String),
    F016 Nullable(Int64),
    F040 Nullable(Int64),
    F021 Nullable(Int64),
    F028 Nullable(Float64),
    F026 Nullable(String),
    F025 Nullable(String)
)
ENGINE = File(Arrow);

-- Force many small Arrow record batches.
SET max_block_size = 1;

INSERT INTO window_parallel_arrow_memory_guard
SELECT
    CAST(NULL, 'Nullable(String)'),
    CAST(NULL, 'Nullable(UInt64)'),
    CAST(NULL, 'Nullable(String)'),
    CAST(NULL, 'Nullable(String)'),
    toNullable(toInt64(number % 100000)),
    toNullable(toInt64(number % 1000)),
    toNullable(toInt64(number % 50000)),
    toNullable(toFloat64(number % 1000) / 10),
    toNullable(if(number % 2 = 0, 'some', 'other')),
    toNullable(toString(number % 100000))
FROM numbers(12000);

SET max_block_size = 65505;
SET max_threads = 32, max_memory_usage = 1500000000;

SELECT '--- explain pipeline ---';
SELECT toUInt8(count() > 0)
FROM
(
    EXPLAIN PIPELINE
    SELECT
        LEAST(F016 - F040, F021) AS AQ,
        F028 * IF(F026 = 'some', 1, -1) AS SBP,
        SUM(AQ) OVER (PARTITION BY F047, F022, F001, F046 ORDER BY SBP ASC, F025 ASC) AS CSF
    FROM window_parallel_arrow_memory_guard
    LIMIT 1
)
WHERE explain LIKE '%ScatterByPartitionTransform%';

SELECT '--- query execution ---';
SELECT
    LEAST(F016 - F040, F021) AS AQ,
    F028 * IF(F026 = 'some', 1, -1) AS SBP,
    SUM(AQ) OVER (PARTITION BY F047, F022, F001, F046 ORDER BY SBP ASC, F025 ASC) AS CSF
FROM window_parallel_arrow_memory_guard
LIMIT 1
FORMAT Null;

SELECT 'OK';

DROP TABLE window_parallel_arrow_memory_guard;
