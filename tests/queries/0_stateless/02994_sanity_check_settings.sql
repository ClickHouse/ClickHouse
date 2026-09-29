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


SET max_threads = 9223372036854775807;
-- The clamp must reduce the requested 2^63-1 to EXACTLY 256 * getNumberOfCPUCoresToUse().
-- The core count is derived at runtime from the still-visible `default` column (`auto(N)`),
-- which is unaffected by the SET above, so the bound tracks the real clamp contract on any
-- host instead of relying on a hardcoded threshold. The only literal is the documented 256
-- multiplier from src/Core/SettingsQuirks.cpp:113.
SELECT
    toUInt64(value) = 256 * toUInt64(extract(default, 'auto\\(([0-9]+)\\)')) AS clamped_to_256x_cores,
    toUInt64(value) < toUInt64(9223372036854775807) AS reduced_from_requested
FROM system.settings
WHERE name = 'max_threads';