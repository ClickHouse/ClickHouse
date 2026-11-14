-- Tags: no-random-settings, no-sanitizers
-- no-sanitizers: the retry must fit in the time the first attempt leaves of `max_execution_time`, which sanitizer builds
-- exceed.

-- Test: ABR engine fallback when TIMEOUT_EXCEEDED occurs
-- Verifies that ABR retries on the next table when max_execution_time
-- causes the first (finer) table to time out during execution.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS timeout_test_1;
DROP TABLE IF EXISTS timeout_test_10;

-- Table 1: High resolution, many rows
CREATE TABLE timeout_test_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMinute(ts)
ORDER BY ts;

-- Table 2: Low resolution, very few rows
CREATE TABLE timeout_test_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toStartOfMonth(ts)
ORDER BY ts;

-- A small fixture keeps setup and the successful retry cheap, including under sanitizers.
INSERT INTO timeout_test_1
SELECT
    1,
    toDateTime('2026-01-01 12:00:00') - INTERVAL number second,
    sipHash64(number)
FROM numbers(2001);

-- Insert 11 rows.
INSERT INTO timeout_test_10
SELECT
    10,
    toDateTime('2026-01-01 12:00:00') - INTERVAL number * 5_000_000 second,
    sipHash64(number)
FROM numbers(11);

-- Create the ABR table
CREATE TABLE abr ENGINE = ABR('timeout_test', [1, 10], ('ts'));

-- The fine tier sleeps for about 20 seconds, exceeding its half of the 10-second budget.
-- Minute partitions bound each block to 60 rows, below the per-block sleep limit.
-- This leaves time for the one-row retry on the coarse tier.
SELECT /* test 05304, query 1 */ count(), sum(_sample_interval) FROM abr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second AND sleepEachRow(0.01) = 0
SETTINGS max_execution_time = 10, max_threads = 1, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;

-- abr_interval should be 10 (the retry landed on timeout_test_10), and abr_exception_codes
-- should show that the attempt on timeout_test_1 failed with TIMEOUT_EXCEEDED
SELECT abr_interval, mapApply((k, v) -> (k, errorCodeToName(v)), abr_exception_codes) AS exception_codes FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05304, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: Same query but without time limit, should use timeout_test_1 and finish quickly
SELECT /* test 05304, query 2 */ count(), sum(_sample_interval) FROM abr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second;

SYSTEM FLUSH LOGS query_log;

-- abr_interval should be 1 (no retry)
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05304, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: the share of an attempt also covers the work after reading. The fine tier reads its rows at once, but its
-- ~200 groups then sleep for about 6 seconds in HAVING, more than its 5-second share, so it is retried on the coarse tier.
-- The groups come in blocks of about 10 rows, so the attempt is stopped soon after its share is used up, which leaves
-- the coarse tier most of the remaining 5 seconds. With parallel replicas, the merged groups come in a single block.
SELECT /* test 05304, query 3 */ max(_sample_interval) FROM
(
    SELECT _sample_interval, value % 200 AS k FROM abr
    WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second
    GROUP BY _sample_interval, k
    HAVING sleepEachRow(0.03) + count() > 0
)
SETTINGS max_execution_time = 10, max_threads = 1, max_block_size = 10, enable_parallel_replicas = 0, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;

SELECT abr_interval, mapApply((k, v) -> (k, errorCodeToName(v)), abr_exception_codes) AS exception_codes FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05304, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 4: an attempt that stopped with a partial result would not be retried, so 'break' is rejected for a limit that is set.
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second SETTINGS max_execution_time = 10, timeout_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second SETTINGS max_rows_to_read = 100000, read_overflow_mode = 'break', enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
-- Without these limits 'break' is accepted. They are reset explicitly, as the test server may set them in the profile of the user.
SELECT count() > 0 FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 second
SETTINGS max_execution_time = 0, max_rows_to_read = 0, max_bytes_to_read = 0, timeout_overflow_mode = 'break', read_overflow_mode = 'break';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS timeout_test_1;
DROP TABLE IF EXISTS timeout_test_10;
