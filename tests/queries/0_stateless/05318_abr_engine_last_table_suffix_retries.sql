-- Test: abr_last_table_suffix bounds the retries of the ABR engine, and the split of the limits.
-- The retries after TOO_MANY_ROWS do not go beyond the table of abr_last_table_suffix, and the read
-- limits are split only between the candidate tables up to it.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS last_suffix_1;
DROP TABLE IF EXISTS last_suffix_10;

CREATE TABLE last_suffix_1 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE last_suffix_10 (
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- 1000 hourly rows: a query on the last 950 hours reads about 1000 rows from this table.
INSERT INTO last_suffix_1
SELECT toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR, 1
FROM system.numbers
LIMIT 1000;

-- 500 rows every 10 hours: the same query reads at most 500 rows from this table.
INSERT INTO last_suffix_10
SELECT toDateTime('2025-11-11 12:00:00') - INTERVAL number * 10 HOUR, 10
FROM system.numbers
LIMIT 500;

CREATE TABLE abr ENGINE = ABR('last_suffix', [1, 10], ('ts'));

-- The queries use sum(value), which has to read the rows: count() could be answered by the implicit projections.
-- sum(value) is 951 on interval 1 (951 rows of value 1) and 960 on interval 10 (96 rows of value 10).

-- (1) Without abr_last_table_suffix both tables are candidates, so each attempt may read 1500 / 2 = 750 rows:
-- the attempt on interval 1 fails with TOO_MANY_ROWS and the query is retried on interval 10.
SELECT /* test 05318, query 1 */ sum(value) FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 950 HOUR
SETTINGS max_rows_to_read = 1500, send_logs_level = 'error';

-- (2) With abr_last_table_suffix = 1 the only candidate is interval 1, which gets the whole limit of 1500 rows.
SELECT /* test 05318, query 2 */ sum(value) FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 950 HOUR
SETTINGS max_rows_to_read = 1500, abr_last_table_suffix = 1, send_logs_level = 'error';

-- (3) With abr_last_table_suffix = 1 and a limit that interval 1 exceeds, the query is not retried on interval 10.
SELECT /* test 05318, query 3 */ sum(value) FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 950 HOUR
SETTINGS max_rows_to_read = 900, abr_last_table_suffix = 1, send_logs_level = 'fatal'; -- { serverError TOO_MANY_ROWS }

SYSTEM FLUSH LOGS query_log;

SELECT
    type,
    abr_interval,
    mapApply((k, v) -> (k, errorCodeToName(v)), abr_exception_codes) AS exception_codes
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT /* test 05318, query _ */%'
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND event_date >= yesterday()
ORDER BY query LIKE '%query 1 */%' DESC, query LIKE '%query 2 */%' DESC, event_time;

DROP TABLE abr;
DROP TABLE last_suffix_1;
DROP TABLE last_suffix_10;
