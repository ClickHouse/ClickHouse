-- Regression test: PREWHERE with IN subquery on a primary key column must use
-- primary key analysis for granule pruning, same as WHERE.
--
-- The bug: buildSetsForDAG() for PREWHERE calls buildSetInplace() which creates
-- the set without storing explicit set elements. When KeyCondition later calls
-- buildOrderedSetInplace(), it sees the set is already created, checks
-- hasExplicitSetElements() -> false, and returns nullptr. The IN condition is
-- then excluded from primary key analysis, so all granules are read.
--
-- With WHERE the set is not pre-built, so buildOrderedSetInplace() builds it
-- from scratch with fillSetElements() and the IN condition works for index.

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS ids;

CREATE TABLE data (id UInt64, ts DateTime, value Float64)
ENGINE = MergeTree ORDER BY (id, ts);

CREATE TABLE ids (id UInt64)
ENGINE = MergeTree ORDER BY id;

-- 100 distinct ids, 8192 rows per id.
-- The absolute number of granules selected depends on randomized settings
-- (index_granularity, max_insert_threads, min_bytes_for_wide_part, ...),
-- so this test does not pin it. It compares PREWHERE vs WHERE instead.
INSERT INTO data SELECT number % 100, toDateTime('2020-01-01') + intDiv(number, 100), number FROM numbers(819200);

OPTIMIZE TABLE data FINAL;

INSERT INTO ids VALUES (1);

CREATE TEMPORARY TABLE start_ts AS (SELECT now() AS ts);

-- WHERE variant: uses primary key analysis via buildOrderedSetInplace() that
-- builds the subquery set from scratch with explicit elements.
SELECT count()
FROM data
WHERE id IN (SELECT id FROM ids) AND ts >= '2020-01-01' AND ts <= '2020-12-31'
SETTINGS log_comment = '04097_where'
FORMAT Null;

-- PREWHERE variant: must pick the same granules as WHERE above.
SELECT count()
FROM data
PREWHERE id IN (SELECT id FROM ids) AND ts >= '2020-01-01' AND ts <= '2020-12-31'
SETTINGS log_comment = '04097_prewhere'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Compare SelectedMarks for PREWHERE vs WHERE directly. Both queries read the
-- same parts with the same granularity, so primary key analysis must select
-- the same granules for both. This is robust under CI randomization of
-- index_granularity / max_insert_threads / min_bytes_for_wide_part.
--
-- Without the fix, PREWHERE reads every granule (hundreds of marks) while WHERE
-- prunes down to the granules covering id=1, so prewhere_marks >> where_marks.
WITH
    (
        SELECT ProfileEvents['SelectedMarks']
        FROM system.query_log
        WHERE type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND event_date >= yesterday()
            AND event_time >= (SELECT ts FROM start_ts)
            AND log_comment = '04097_where'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS where_marks,
    (
        SELECT ProfileEvents['SelectedMarks']
        FROM system.query_log
        WHERE type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND event_date >= yesterday()
            AND event_time >= (SELECT ts FROM start_ts)
            AND log_comment = '04097_prewhere'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS prewhere_marks
SELECT if(prewhere_marks <= where_marks,
          'ok',
          format('error: PREWHERE selected {} marks, WHERE selected {} marks',
                 toString(prewhere_marks), toString(where_marks)));

DROP TABLE data;
DROP TABLE ids;
