-- Tags: no-tsan, no-asan, no-msan, no-replicated-database, no-random-settings
-- Tag no-tsan: Fine thresholds on memory usage
-- Tag no-asan: Fine thresholds on memory usage
-- Tag no-msan: Fine thresholds on memory usage

-- each uniqCombined state should not use > sizeof(HLL) in memory,
-- sizeof(HLL) is (2^K * 6 / 8)
-- hence max_memory_usage for 100 rows = (96<<10)*100 = 9830400

SET use_uncompressed_cache = 0;
SET memory_profiler_step = 1;

-- Pin `max_threads = 1` so the server-level `additional_memory_tracking_per_thread`
-- speculative reservation (4 MiB by default) contributes one fixed offset
-- per query against `max_memory_usage`. The success-side thresholds are
-- offset by +4 MiB to absorb that reservation; the failure-side thresholds
-- are left as-is — the queries are still expected to exceed the limit.
SET max_threads = 1;

-- HashTable for UInt32 (used until (1<<13) elements), hence 8192 elements
SELECT 'UInt32';
SET max_memory_usage = 4000000;
SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 14025216; -- 9830400 + 4 MiB
SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(number % 8192) u FROM numbers(8192 * 100) GROUP BY k);

-- HashTable for UInt64 (used until (1<<12) elements), hence 4096 elements
SELECT 'UInt64';
SET max_memory_usage = 4000000;
SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(reinterpretAsString(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 14025216;


SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(reinterpretAsString(number % 4096)) u FROM numbers(4096 * 100) GROUP BY k);

SELECT 'K=16';

-- HashTable for UInt32 (used until (1<<12) elements), hence 4096 elements
SELECT 'UInt32';
SET max_memory_usage = 2000000;
SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 9424816; -- 5230000 + 4 MiB
SELECT sum(u) FROM (SELECT intDiv(number, 4096) AS k, uniqCombined(16)(number % 4096) u FROM numbers(4096 * 100) GROUP BY k);

-- HashTable for UInt64 (used until (1<<11) elements), hence 2048 elements
SELECT 'UInt64';
SET max_memory_usage = 2000000;
SELECT sum(u) FROM (SELECT intDiv(number, 2048) AS k, uniqCombined(16)(reinterpretAsString(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 10094816; -- 5900000 + 4 MiB
SELECT sum(u) FROM (SELECT intDiv(number, 2048) AS k, uniqCombined(16)(reinterpretAsString(number % 2048)) u FROM numbers(2048 * 100) GROUP BY k);

SELECT 'K=18';

-- HashTable for UInt32 (used until (1<<14) elements), hence 16384 elements
SELECT 'UInt32';
SET max_memory_usage = 8000000;
SELECT sum(u) FROM (SELECT intDiv(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 23855616; -- 19660800 + 4 MiB
SELECT sum(u) FROM (SELECT intDiv(number, 16384) AS k, uniqCombined(18)(number % 16384) u FROM numbers(16384 * 100) GROUP BY k);

-- HashTable for UInt64 (used until (1<<13) elements), hence 8192 elements
SELECT 'UInt64';
SET max_memory_usage = 8000000;
SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(18)(reinterpretAsString(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k); -- { serverError MEMORY_LIMIT_EXCEEDED }
SET max_memory_usage = 23855616;
SELECT sum(u) FROM (SELECT intDiv(number, 8192) AS k, uniqCombined(18)(reinterpretAsString(number % 8192)) u FROM numbers(8192 * 100) GROUP BY k);
