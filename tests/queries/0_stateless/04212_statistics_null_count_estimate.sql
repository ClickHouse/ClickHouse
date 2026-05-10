-- Test: Verify system.parts_columns.estimates.null_count shows correct NULL counts
-- Covers: src/Storages/System/StorageSystemPartsColumns.cpp:327-329

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS test_nullcount_estimate;
CREATE TABLE test_nullcount_estimate (
    id UInt64,
    col_10pct Nullable(Int64),       -- 10% NULL
    col_50pct Nullable(Int64),       -- 50% NULL
    col_90pct Nullable(Int64),       -- 90% NULL
    col_non_null Int64               -- 0% NULL (not Nullable)
) ENGINE = MergeTree() ORDER BY id
SETTINGS auto_statistics_types = 'nullcount,minmax';

-- Insert 100 rows with known NULL distribution
INSERT INTO test_nullcount_estimate SELECT
    number,
    if(number % 10 = 0, NULL, number),    -- 10 NULLs
    if(number % 2 = 0, NULL, number),     -- 50 NULLs
    if(number % 10 != 0, NULL, number),   -- 90 NULLs
    number
FROM numbers(100);

-- Verify statistics types are correct
SELECT 'Statistics types:';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_estimate'
  AND active AND column LIKE 'col_%'
ORDER BY column;

-- Verify estimates.null_count shows correct values
SELECT 'Null count estimates:';
SELECT column, `estimates.null_count`
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_estimate'
  AND active AND column LIKE 'col_%'
ORDER BY column;

DROP TABLE test_nullcount_estimate;
