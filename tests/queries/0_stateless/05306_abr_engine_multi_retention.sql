-- Test: ABR engine multi-dimensional retention expression
-- Verifies that ABR correctly uses a multi-column retention expression ('ts', 'region')
-- to prune partitions by non-time dimensions during table selection.
--
-- Setup: table_1 has 3 days of 'us' data but 30 days of 'eu' data.
-- A 5-day lookback query for region='us' should skip table_1 (not enough 'us' data),
-- while the same query for region='eu' should select table_1.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_mr;
DROP TABLE IF EXISTS multi_ret_1;
DROP TABLE IF EXISTS multi_ret_10;

CREATE TABLE multi_ret_1 (
    _sample_interval UInt64 DEFAULT 1,
    region String,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), region)
ORDER BY ts;

CREATE TABLE multi_ret_10 (
    _sample_interval UInt64 DEFAULT 10,
    region String,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), region)
ORDER BY ts;

-- table_1: 72 hourly rows for 'us' (3 days), 720 hourly rows for 'eu' (30 days)
INSERT INTO multi_ret_1
SELECT 1, 'us', toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 72;

INSERT INTO multi_ret_1
SELECT 1, 'eu', toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 720;

-- table_10: 720 hourly rows for both regions (30 days each)
INSERT INTO multi_ret_10
SELECT 10, 'us', toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 720;

INSERT INTO multi_ret_10
SELECT 10, 'eu', toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, number
FROM system.numbers LIMIT 720;

-- Multi-dimensional retention expression: prune by both ts and region
CREATE TABLE abr_mr
ENGINE = ABR('multi_ret', [1, 10], ('ts', 'region'));

-- Test 1: region='us', 5-day lookback
-- table_1 only has 3 days of 'us' data → ABR prunes 'eu' parts, sees min_date too recent → skips to table_10
-- abr_interval = 10
SELECT /* test 05306, query 1 */ count() FROM abr_mr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 5 DAY
  AND region = 'us';

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05306, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: region='eu', 5-day lookback
-- table_1 has 30 days of 'eu' data → ABR prunes 'us' parts, sees enough 'eu' data → selects table_1
-- abr_interval = 1
SELECT /* test 05306, query 2 */ count() FROM abr_mr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 5 DAY
  AND region = 'eu';

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05306, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: no region filter, 5-day lookback
-- No pruning by region → min_date comes from 'eu' parts (30 days) → table_1 selected
-- abr_interval = 1
SELECT /* test 05306, query 3 */ count() FROM abr_mr
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 5 DAY;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05306, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr_mr;
DROP TABLE IF EXISTS multi_ret_1;
DROP TABLE IF EXISTS multi_ret_10;
