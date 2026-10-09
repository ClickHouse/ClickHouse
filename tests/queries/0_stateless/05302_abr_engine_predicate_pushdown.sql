-- Test: ABR engine predicate pushdown to underlying MergeTree tables
-- Verifies that WHERE clause predicates are correctly pushed down to underlying
-- tables by using max_rows_to_read as a gate: if pushdown works, only the
-- filtered partition is read and the query stays under max_rows_to_read.
-- If pushdown is broken, a full scan triggers TOO_MANY_ROWS causing ABR to
-- fall back to a coarser table, detected via abr_interval.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS predicate_test_1;
DROP TABLE IF EXISTS predicate_test_10;

-- Table 1: High resolution (sample_interval = 1)
CREATE TABLE predicate_test_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    account_id String,
    user_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), account_id)
ORDER BY (ts, user_id);

-- Table 2: Low resolution (sample_interval = 10)
CREATE TABLE predicate_test_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    account_id String,
    user_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY (toStartOfMonth(ts), account_id)
ORDER BY (ts, user_id);

-- Insert data for multiple accounts and users
-- Account 'A' with 1000 rows, Account 'B' with 1000 rows
INSERT INTO predicate_test_1
SELECT
    1,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    if(number % 2 = 0, 'account_A', 'account_B'),
    concat('user_', toString(number % 10)),
    number
FROM system.numbers
LIMIT 2000;

-- predicate_test_10 spans ~4000h so its retention is strictly greater than
-- predicate_test_1's (~2000h). This lets Q2's 3000h time range be covered by
-- table_10 directly via the chooseBestTable cover loop (without relying on the
-- equal-retention fallback).
INSERT INTO predicate_test_10
SELECT
    10,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number * 10 HOUR,
    if(number % 2 = 0, 'account_A', 'account_B'),
    concat('user_', toString(number % 10)),
    number * 10
FROM system.numbers
LIMIT 400;

-- Create the ABR table
CREATE TABLE abr
ENGINE = ABR('predicate_test', [1, 10], ('ts'));

-- Test 1: account_id partition filter on table_1
-- predicate_test_1 has 2000 rows, ~1000 per account (partitioned by account_id)
-- max_rows_to_read = 2200, so 1100 per attempt with the two candidate tables (the read limits are split
-- equally between the candidate tables): with pushdown reads ~1000 (OK), without reads 2000 (TOO_MANY_ROWS → fallback)
-- If pushdown works: abr_interval = 1; if broken: abr_interval = 10
SELECT /* test 05302, query 1 */ count() FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 1999 HOUR
  AND account_id = 'account_A'
SETTINGS max_rows_to_read = 2200;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05302, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: account_id partition filter, wider time range routes to table_10
-- predicate_test_10 has 400 rows (~200 per account) spanning ~4000h. Q2's
-- 3000h range is not covered by table_1 (~2000h retention) but is covered by
-- table_10, so chooseBestTable's cover loop routes here. account_A in the
-- 3000h window holds 151 rows.
-- max_rows_to_read = 300, all of it for the only candidate table: with pushdown only account_A partitions are read
-- (~200 rows, OK); without pushdown the full table is read (~400 rows, fails
-- with no more tables to fall back to).
SELECT /* test 05302, query 2 */ count() FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3000 HOUR
  AND account_id = 'account_A'
SETTINGS max_rows_to_read = 300;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05302, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: multiple predicates (account_id AND user_id) on table_1
-- account_A AND user_0 matches ~200 rows, well under the 1100 limit
-- Verifies that multiple non-time predicates are all pushed down
SELECT /* test 05302, query 3 */ count() FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 1999 HOUR
  AND account_id = 'account_A'
  AND user_id = 'user_0'
SETTINGS max_rows_to_read = 2200;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05302, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS predicate_test_1;
DROP TABLE IF EXISTS predicate_test_10;
