-- Regression test: PREWHERE with IN subquery on a primary key column must use
-- primary key analysis for granule pruning, same as WHERE.
--
-- The bug: buildSetsForDAG() for PREWHERE calls buildSetInplace() which creates
-- the set without storing explicit set elements. When KeyCondition later calls
-- buildOrderedSetInplace(), it sees the set is already created, checks
-- hasExplicitSetElements() → false, and returns nullptr. The IN condition is
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

-- 100 distinct ids × 8192 rows each = 819200 rows = 100 granules (one per id).
INSERT INTO data SELECT number % 100, toDateTime('2020-01-01') + intDiv(number, 100), number FROM numbers(819200);

INSERT INTO ids VALUES (1);

CREATE TEMPORARY TABLE start_ts AS (SELECT now() AS ts);

-- WHERE variant: should read very few marks (only granules for id=1).
SELECT count()
FROM data
WHERE id IN (SELECT id FROM ids) AND ts >= '2020-01-01' AND ts <= '2020-12-31'
SETTINGS log_comment = '04097_where'
FORMAT Null;

-- PREWHERE variant: must also read the same few marks.
SELECT count()
FROM data
PREWHERE id IN (SELECT id FROM ids) AND ts >= '2020-01-01' AND ts <= '2020-12-31'
SETTINGS log_comment = '04097_prewhere'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Both WHERE and PREWHERE should select the same number of marks (just 1 granule for id=1).
-- Without the fix, PREWHERE reads all 100 marks instead of 1.
SELECT
    log_comment,
    if(ProfileEvents['SelectedMarks'] <= 2, 'ok',
       format('error: SelectedMarks={} (expected <=2), query_id={}', ProfileEvents['SelectedMarks'], query_id))
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND event_date >= yesterday()
    AND event_time >= (SELECT ts FROM start_ts)
    AND log_comment IN ('04097_where', '04097_prewhere')
ORDER BY log_comment;

DROP TABLE data;
DROP TABLE ids;
