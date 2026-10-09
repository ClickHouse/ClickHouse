-- Tags: no-random-settings
-- ^ the timing of the test depends on reading the parts one after another

-- Test: the execution time of an attempt is the time the query has left, divided by the candidate tables left.
--
-- The attempt on adaptive_budget_1 fails at once with TOO_MANY_ROWS (101 rows > 50, max_rows_to_read is split
-- between the three candidate tables). The attempt on adaptive_budget_10 sleeps 0.1 s for each of its 41 rows,
-- about 4.1 s with max_threads = 1. It gets the remaining ~10 s divided by the two candidate tables left, about 5 s, and succeeds.
-- With an equal split of max_execution_time it would only get 10 / 3 = 3.3 s, time out, and fall back to
-- adaptive_budget_100.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS adaptive_budget_abr;
DROP TABLE IF EXISTS adaptive_budget_1;
DROP TABLE IF EXISTS adaptive_budget_10;
DROP TABLE IF EXISTS adaptive_budget_100;

CREATE TABLE adaptive_budget_1
(
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(ts)
ORDER BY ts
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE adaptive_budget_10
(
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(ts)
ORDER BY ts
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE adaptive_budget_100
(
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(ts)
ORDER BY ts
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO adaptive_budget_1
SELECT 1, toDateTime('2026-01-01 12:00:00') - INTERVAL number SECOND, number
FROM numbers(101);

-- Three parts, so that no block sleeps longer than the 3 seconds that sleepEachRow allows.
INSERT INTO adaptive_budget_10
SELECT 10, toDateTime('2026-01-01 12:00:00') - INTERVAL number SECOND, number
FROM numbers(20);

INSERT INTO adaptive_budget_10
SELECT 10, toDateTime('2026-01-01 12:00:00') - INTERVAL (20 + number) SECOND, 20 + number
FROM numbers(20);

INSERT INTO adaptive_budget_10 VALUES
    (10, toDateTime('2026-01-01 11:58:20'), 40);

INSERT INTO adaptive_budget_100 VALUES
    (100, toDateTime('2026-01-01 11:58:20'), 0);

CREATE TABLE adaptive_budget_abr
ENGINE = ABR('adaptive_budget', [1, 10, 100], ('ts'));

SELECT /* test 05345, adaptive budget */ countIf(sleepEachRow(0.1) = 0)
FROM adaptive_budget_abr
WHERE ts >= toDateTime('2026-01-01 11:58:20')
SETTINGS max_rows_to_read = 150, max_execution_time = 10, max_threads = 1, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;

SELECT abr_interval, mapApply((k, v) -> (k, errorCodeToName(v)), abr_exception_codes) AS exception_codes
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE '%SELECT /* test 05345, adaptive budget */%'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE adaptive_budget_abr;
DROP TABLE adaptive_budget_1;
DROP TABLE adaptive_budget_10;
DROP TABLE adaptive_budget_100;
