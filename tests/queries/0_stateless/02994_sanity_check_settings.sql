SET send_logs_level = 'error';
CREATE TABLE data_02052_1_wide0__fuzz_48
(
    `key` Nullable(Int64),
    `value` UInt8
)
    ENGINE = MergeTree
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, allow_nullable_key = 1 AS
SELECT
    number,
    repeat(toString(number), 5)
FROM numbers(1);

-- Disabled because even after reducing internally to "256 * getNumberOfPhysicalCPUCores()" threads it's too much for CI (or for anything running this many times in parallel)
-- SELECT * APPLY max
-- FROM data_02052_1_wide0__fuzz_48
-- GROUP BY key
-- WITH CUBE
-- SETTINGS max_read_buffer_size = 7, max_threads = 9223372036854775807;

SELECT zero + 1 AS x
FROM system.zeros LIMIT 10
    SETTINGS max_block_size = 9223372036854775806, max_rows_to_read = 20, read_overflow_mode = 'break';

EXPLAIN PIPELINE SELECT zero + 1 AS x FROM system.zeros LIMIT 10 SETTINGS max_block_size = 9223372036854775806, max_rows_to_read = 20, read_overflow_mode = 'break';

-- Verify that we clamp odd values to something slightly saner
SET max_block_size = 9223372036854775806;
SELECT value FROM system.settings WHERE name = 'max_block_size';
