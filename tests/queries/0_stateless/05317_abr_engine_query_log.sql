-- Test: ABR engine entries in system.query_log
-- Verifies that abr_duration_ms, abr_exception_codes, abr_interval and abr_min_date are
-- correctly populated in system.query_log for:
--   (1) retention-based selection of the finest table (no fallback),
--   (2) retention-based selection of a coarser table records the skipped
--       finer interval as NO_AVAILABLE_DATA,
--   (3) row-limit fallback from a finer to a coarser table records
--       the failed interval as TOO_MANY_ROWS,
--   (4) non-ABR queries (fields must stay at their defaults),
--   (5) all underlying tables have no parts: every interval must be recorded
--       as NO_AVAILABLE_DATA (the genuine entry for the fallback table must
--       NOT be erased).

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_query_log;
DROP TABLE IF EXISTS abr_query_log_empty;
DROP TABLE IF EXISTS query_log_test_1;
DROP TABLE IF EXISTS query_log_test_10;
DROP TABLE IF EXISTS query_log_empty_1;
DROP TABLE IF EXISTS query_log_empty_10;

CREATE TABLE query_log_test_1 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE query_log_test_10 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Short retention: 500 rows at 1h granularity -> 500h of coverage.
INSERT INTO query_log_test_1
SELECT
    toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR,
    1
FROM system.numbers
LIMIT 500;

-- Long retention: 500 rows at 10h granularity -> 5000h of coverage (10x deeper history).
INSERT INTO query_log_test_10
SELECT
    toDateTime('2026-01-01 12:00:00') - INTERVAL number * 10 HOUR,
    10
FROM system.numbers
LIMIT 500;

CREATE TABLE abr_query_log ENGINE = ABR('query_log_test', [1, 10], ('ts'));

-- (1) Query range fits inside the fine table's retention -> picks interval 1, no fallback.
SELECT /* test 05317, fine-retention */ count() FROM abr_query_log
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 450 HOUR
SETTINGS max_rows_to_read = 2000;

SYSTEM FLUSH LOGS query_log;
SELECT
    abr_interval,
    empty(abr_exception_codes),
    abr_duration_ms > 0,
    abr_min_date
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, fine-retention%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- (2) Query range exceeds the fine table's retention -> ABR picks interval 10 up front.
-- The skipped finer interval (1) must be recorded as NO_AVAILABLE_DATA so the
-- query_log row explains why a coarser table was chosen.
SELECT /* test 05317, coarse-retention */ count() FROM abr_query_log
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2000 HOUR;

SYSTEM FLUSH LOGS query_log;
SELECT
    abr_interval,
    length(abr_exception_codes),
    errorCodeToName(abr_exception_codes[1]) = 'NO_AVAILABLE_DATA',
    abr_duration_ms > 0,
    abr_min_date
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, coarse-retention%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- (3) max_rows_to_read forces fallback from interval 1 to 10.
-- Exception map must contain exactly one entry: {1 -> TOO_MANY_ROWS}.
-- max_rows_to_read is split between the two candidate tables, so each attempt may read 400 rows.
-- (3a) With the implicit projections, the check of the estimate of the rows to read is skipped when the query is planned,
-- so the attempt on interval 1 fails while reading. The rows it read or planned to read must not count against the
-- limit of the attempt on interval 10.
SELECT /* test 05317, fallback-reading */ count() FROM abr_query_log
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 450 HOUR
SETTINGS max_rows_to_read = 800, optimize_use_implicit_projections = 1, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;
SELECT
    abr_interval,
    length(abr_exception_codes),
    errorCodeToName(abr_exception_codes[1]) = 'TOO_MANY_ROWS',
    abr_duration_ms > 0,
    abr_min_date
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, fallback-reading %'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- (3b) Without the implicit projections, the attempt on interval 1 is rejected by the estimate of the rows to read
-- before reading anything.
SELECT /* test 05317, fallback-estimate */ count() FROM abr_query_log
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 450 HOUR
SETTINGS max_rows_to_read = 800, optimize_use_implicit_projections = 0, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;
SELECT
    abr_interval,
    length(abr_exception_codes),
    errorCodeToName(abr_exception_codes[1]) = 'TOO_MANY_ROWS',
    abr_duration_ms > 0,
    abr_min_date
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, fallback-estimate %'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- (4) Non-ABR queries must leave abr_* columns at their defaults (0 / 0 / {} / 0).
SELECT /* test 05317, non-abr */ count() FROM query_log_test_1;

SYSTEM FLUSH LOGS query_log;
SELECT
    abr_interval,
    abr_duration_ms,
    empty(abr_exception_codes),
    toUnixTimestamp(abr_min_date)
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, non-abr%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- (5) Both underlying tables are empty (no parts) -> getMinMergeTreeDate returns
-- nullopt for every table, so every interval is recorded as NO_AVAILABLE_DATA
-- and pickFallbackTable defaults to the finest interval. That fallback table's
-- genuine NO_AVAILABLE_DATA entry must NOT be erased.
CREATE TABLE query_log_empty_1 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE query_log_empty_10 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE abr_query_log_empty ENGINE = ABR('query_log_empty', [1, 10], ('ts'));

SELECT /* test 05317, all-pruned */ count() FROM abr_query_log_empty
WHERE ts >= toDateTime('2026-01-01 00:00:00');

SYSTEM FLUSH LOGS query_log;
-- NOTE: abr_duration_ms is intentionally not asserted here. With both underlying
-- tables empty, chooseBestTable + pipeline initialization can complete in well
-- under one millisecond, so the ms-precision counter legitimately rounds to 0.
-- The other test cases above still verify that abr_duration_ms is recorded when
-- real work occurs.
SELECT
    abr_interval,
    length(abr_exception_codes),
    errorCodeToName(abr_exception_codes[1]) = 'NO_AVAILABLE_DATA',
    errorCodeToName(abr_exception_codes[10]) = 'NO_AVAILABLE_DATA',
    abr_min_date
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%test 05317, all-pruned%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr_query_log;
DROP TABLE IF EXISTS abr_query_log_empty;
DROP TABLE IF EXISTS query_log_test_1;
DROP TABLE IF EXISTS query_log_test_10;
DROP TABLE IF EXISTS query_log_empty_1;
DROP TABLE IF EXISTS query_log_empty_10;
