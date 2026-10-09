-- Test: ABR engine PREWHERE filter extraction and pushdown
-- Verifies that ABR correctly extracts filters from PREWHERE clauses and
-- combines them with WHERE filters for table selection and predicate pushdown.
-- The key code path is buildFilterActionsDAG() which merges prewhere_info
-- into the filter nodes used by chooseBestTable().

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_pw;
DROP TABLE IF EXISTS pw_test_1;
DROP TABLE IF EXISTS pw_test_10;

-- Table 1: High resolution (sample_interval = 1)
CREATE TABLE pw_test_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    account_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), account_id)
ORDER BY ts;

-- Table 2: Low resolution (sample_interval = 10)
CREATE TABLE pw_test_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    account_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), account_id)
ORDER BY ts;

-- 2000 rows in table_1, ~1000 per account
INSERT INTO pw_test_1
SELECT
    1,
    toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR,
    if(number % 2 = 0, 'account_A', 'account_B'),
    number
FROM system.numbers
LIMIT 2000;

-- 200 rows in table_10, ~100 per account
INSERT INTO pw_test_10
SELECT
    10,
    toDateTime('2026-01-01 12:00:00') - INTERVAL number * 10 HOUR,
    if(number % 2 = 0, 'account_A', 'account_B'),
    number * 10
FROM system.numbers
LIMIT 200;

CREATE TABLE abr_pw
ENGINE = ABR('pw_test', [1, 10], ('ts'));

-- Test 1: PREWHERE time filter only
-- If PREWHERE is extracted: ABR sees time bound, selects table_1 → abr_interval = 1
-- If PREWHERE is NOT extracted: no time bound → fallback to table_10 → abr_interval = 10
SELECT /* test 05305, query 1 */ count() FROM abr_pw
PREWHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 1999 HOUR;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05305, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: PREWHERE non-time filter + WHERE time filter → pushdown check
-- table_1 has 2000 rows, ~1000 per account. max_rows_to_read = 2200, so 1100 per attempt with the two
-- candidate tables (the read limits are split equally between the candidate tables).
-- With PREWHERE pushdown: reads ~1000 rows (account_A) → OK → abr_interval = 1
-- Without pushdown: reads 2000 → TOO_MANY_ROWS → fallback → abr_interval = 10
SELECT /* test 05305, query 2 */ count() FROM abr_pw
PREWHERE account_id = 'account_A'
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 1999 HOUR
SETTINGS max_rows_to_read = 2200;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05305, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: PREWHERE time filter + WHERE non-time filter → both extracted
-- Time from PREWHERE used for table selection, non-time from WHERE pushed down.
-- With PREWHERE time extraction: selects table_1, WHERE prunes to ~1000 rows → OK
-- Without PREWHERE time extraction: falls back to table_10 → abr_interval = 10
SELECT /* test 05305, query 3 */ count() FROM abr_pw
PREWHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 1999 HOUR
WHERE account_id = 'account_A'
SETTINGS max_rows_to_read = 2200;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05305, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 4: WHERE and PREWHERE both with time filters (PREWHERE tighter)
-- WHERE ts >= Dec 22, PREWHERE ts >= Dec 26 → combined lower bound = Dec 26
-- abr_min_date should be 1766707200 (2025-12-26 00:00:00 UTC)
SELECT /* test 05305, query 4 */ count() FROM abr_pw
PREWHERE ts >= toDateTime('2025-12-26 00:00:00')
WHERE ts >= toDateTime('2025-12-22 00:00:00');

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05305, query 4 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 5: WHERE and PREWHERE both with time filters (WHERE tighter)
-- WHERE ts >= Dec 26, PREWHERE ts >= Dec 22 → combined lower bound = Dec 26
-- abr_min_date should be 1766707200 (2025-12-26 00:00:00 UTC)
SELECT /* test 05305, query 5 */ count() FROM abr_pw
PREWHERE ts >= toDateTime('2025-12-22 00:00:00')
WHERE ts >= toDateTime('2025-12-26 00:00:00');

SYSTEM FLUSH LOGS query_log;
SELECT toUnixTimestamp(abr_min_date) as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05305, query 5 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr_pw;
DROP TABLE IF EXISTS pw_test_1;
DROP TABLE IF EXISTS pw_test_10;
