-- Test: getMinMergeTreeDate must not be biased by the query's timestamp range.
--
-- Reproduces a production bug where retention_expression = (timestamp, namespace)
-- caused the timestamp predicates from WHERE to leak into the retention_condition
-- and ABR engine would wrongly skip tables
--
-- Setup:
--   retention_bug_1
--       namespace 1 has data from 2025-06-01 to 2026-03-09, but there is a
--       gap on Feb 1st (data resumes Feb 2nd). So the February partition's
--       min timestamp = 2026-02-02, not 2026-02-01.
--
--       namespace 2 has continuous data from 2025-06-01 to 2026-03-09.
--
--   retention_bug_10
--       both namespaces have full continuous coverage.
--
-- Query:   WHERE timestamp >= '2026-02-01' AND namespace = 1
-- Correct: Chooses retention_bug_1
-- Bug:     If timestamp is considered in part pruning, it will wrongly skip to retention_bug_10

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_retention_bug;
DROP TABLE IF EXISTS retention_bug_1;
DROP TABLE IF EXISTS retention_bug_10;

CREATE TABLE retention_bug_1 (
    _sample_interval UInt64 DEFAULT 1,
    timestamp DateTime,
    namespace UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(timestamp), namespace)
ORDER BY timestamp;

CREATE TABLE retention_bug_10 (
    _sample_interval UInt64 DEFAULT 10,
    timestamp DateTime,
    namespace UInt64,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(timestamp), namespace)
ORDER BY timestamp;

-- retention_bug_1 
--   namespace 1: June 1 through Jan 31 (hourly), then Feb 2 through Mar 9.
--   Feb 1st is intentionally missing so the February partition min = Feb 2.
INSERT INTO retention_bug_1
SELECT 1,
    toDateTime('2026-01-31 23:00:00') - INTERVAL number HOUR,
    1,
    number
FROM system.numbers LIMIT 5880;  -- 2025-06-01 00:00 → 2026-01-31 23:00

INSERT INTO retention_bug_1
SELECT 1,
    toDateTime('2026-03-09 00:00:00') - INTERVAL number HOUR,
    1,
    number
FROM system.numbers LIMIT 841;  -- 2026-02-02 00:00 → 2026-03-09 00:00

-- retention_bug_1
--   namespace 2: continuous from June 1 to Mar 9
INSERT INTO retention_bug_1
SELECT 1,
    toDateTime('2026-03-09 00:00:00') - INTERVAL number HOUR,
    2,
    number
FROM system.numbers LIMIT 6721;  -- 2025-06-01 00:00 → 2026-03-09 00:00

-- retention_bug_10
--   both namespaces with full continuous coverage
INSERT INTO retention_bug_10
SELECT 10,
    toDateTime('2026-03-09 00:00:00') - INTERVAL number HOUR,
    1,
    number
FROM system.numbers LIMIT 6721;

INSERT INTO retention_bug_10
SELECT 10,
    toDateTime('2026-03-09 00:00:00') - INTERVAL number HOUR,
    2,
    number
FROM system.numbers LIMIT 6721;

CREATE TABLE abr_retention_bug
ENGINE = ABR('retention_bug', [1, 10], ('timestamp', 'namespace'));

-- Test 1: time range from Feb 1 + namespace filter.
-- retention_bug_1 has ns=1 data back to June 2025 — plenty of depth.
-- Bug: timestamp >= Feb 1 leaks into retention pruning, only Feb + Mar parts survive
--      min_date = Feb 2 > query lower bound Feb 1 → table_1 wrongly skipped.
SELECT /* test 05308, query 1 */
    count() AS requests
FROM abr_retention_bug
WHERE timestamp >= toDateTime('2026-02-01 00:00:00')
  AND namespace = 1;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT /* test 05308, query 1 */%'
  AND type = 2 AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

-- Test 2: same time range, namespace 2 (continuous data, no gap) -> table_1
SELECT /* test 05308, query 2 */
    count() AS requests
FROM abr_retention_bug
WHERE timestamp >= toDateTime('2026-02-01 00:00:00')
  AND namespace = 2;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT /* test 05308, query 2 */%'
  AND type = 2 AND event_date >= yesterday()
ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr_retention_bug;
DROP TABLE IF EXISTS retention_bug_1;
DROP TABLE IF EXISTS retention_bug_10;