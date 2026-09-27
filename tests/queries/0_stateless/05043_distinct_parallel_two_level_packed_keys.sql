-- Tags: long
-- Packed fixed-width composite keys (`keys32` for <= 4 key bytes, `keys64` for <= 8) must honor the
-- two-level DISTINCT settings. Before these carriers existed the settings silently did nothing for
-- common multi-column DISTINCTs like `(UInt16, UInt16)` or `(UInt32, UInt32)`: the set stayed
-- single-level and the per-bucket parallel build never ran. This test proves both that the parallel
-- build now fires for these methods (`DistinctTwoLevelParallelFilterBuilds > 0`) and that its result
-- matches the serial path (threshold 0 disables promotion). Booleans are emitted so no distinct-count
-- magic constant is needed.
-- The probes pin `max_block_size` and `distinct_two_level_parallel_build_min_rows`: the parallel build
-- needs a chunk big enough for more than one worker, so a randomized smaller block would correctly
-- take the serial path and the assertions would not be testing what they name.

SET max_threads = 8;

-- keys32: two UInt16 columns pack into 4 bytes -> Type::keys32
SELECT DISTINCT (number % 60000)::UInt16 AS a, (number % 40000)::UInt16 AS b FROM numbers_mt(4000000)
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000, max_block_size = 65409,
                         distinct_two_level_parallel_build_min_rows = 10000,
                         log_comment = '05042_distinct_keys32_probe';

-- keys64: two UInt32 columns pack into 8 bytes -> Type::keys64
SELECT DISTINCT (number % 300000)::UInt32 AS a, (number % 200000)::UInt32 AS b FROM numbers_mt(4000000)
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000, max_block_size = 65409,
                         distinct_two_level_parallel_build_min_rows = 10000,
                         log_comment = '05042_distinct_keys64_probe';

SYSTEM FLUSH LOGS query_log;

-- The packed-key two-level parallel build actually ran (settings honored, not a silent no-op).
SELECT ProfileEvents['DistinctTwoLevelParallelFilterBuilds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05042_distinct_keys32_probe' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['DistinctTwoLevelParallelFilterBuilds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05042_distinct_keys64_probe' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Two-level result MUST match the serial path (keys32 count).
SELECT
(
    SELECT count() FROM (SELECT DISTINCT (number % 60000)::UInt16 AS a, (number % 40000)::UInt16 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000
) = (
    SELECT count() FROM (SELECT DISTINCT (number % 60000)::UInt16 AS a, (number % 40000)::UInt16 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 0
);

-- Two-level result MUST match the serial path (keys64 count).
SELECT
(
    SELECT count() FROM (SELECT DISTINCT (number % 300000)::UInt32 AS a, (number % 200000)::UInt32 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000
) = (
    SELECT count() FROM (SELECT DISTINCT (number % 300000)::UInt32 AS a, (number % 200000)::UInt32 AS b FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 0
);

-- Content digest of the distinct pairs must survive intact (keys32): promoted vs serial.
SELECT
(
    SELECT sum(cityHash64(a, b)) FROM (SELECT DISTINCT (number % 60000)::UInt16 AS a, (number % 40000)::UInt16 AS b FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 1000
) = (
    SELECT sum(cityHash64(a, b)) FROM (SELECT DISTINCT (number % 60000)::UInt16 AS a, (number % 40000)::UInt16 AS b FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 0
);

-- Content digest (keys64).
SELECT
(
    SELECT sum(cityHash64(a, b)) FROM (SELECT DISTINCT (number % 300000)::UInt32 AS a, (number % 200000)::UInt32 AS b FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 1000
) = (
    SELECT sum(cityHash64(a, b)) FROM (SELECT DISTINCT (number % 300000)::UInt32 AS a, (number % 200000)::UInt32 AS b FROM numbers_mt(2000000)) SETTINGS distinct_two_level_threshold = 0
);
