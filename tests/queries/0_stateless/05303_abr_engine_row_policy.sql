-- Test: ABR engine row policy enforcement on underlying tables
-- Verifies that row policies defined on underlying tables are correctly
-- applied when querying through the ABR engine.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_policy_test_1;
DROP TABLE IF EXISTS row_policy_test_10;
DROP ROW POLICY IF EXISTS 03586_filter_value ON row_policy_test_1;
DROP ROW POLICY IF EXISTS 03586_filter_value ON row_policy_test_10;
DROP ROW POLICY IF EXISTS 03586_filter_account ON row_policy_test_1;
DROP ROW POLICY IF EXISTS 03586_filter_account ON row_policy_test_10;

-- Table 1: High resolution (sample_interval = 1)
CREATE TABLE row_policy_test_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    account_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 2: Low resolution (sample_interval = 10)
CREATE TABLE row_policy_test_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    account_id String,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Insert data: 100 rows per table, values 0..99
INSERT INTO row_policy_test_1
SELECT 1, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR,
       if(number % 2 = 0, 'acct_A', 'acct_B'), number
FROM system.numbers LIMIT 100;

INSERT INTO row_policy_test_10
SELECT 10, toDateTime('2026-01-01 12:00:00') - INTERVAL number * 10 HOUR,
       if(number % 2 = 0, 'acct_A', 'acct_B'), number
FROM system.numbers LIMIT 100;

CREATE TABLE abr ENGINE = ABR('row_policy_test', [1, 10], ('ts'));

-- Test 1: No row policy - baseline counts
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR;
SELECT count() FROM row_policy_test_1;

-- Test 2: Row policy filtering on value column (value < 50)
-- Applied to BOTH underlying tables
CREATE ROW POLICY 03586_filter_value ON row_policy_test_1 USING value < 50 AS permissive TO ALL;
CREATE ROW POLICY 03586_filter_value ON row_policy_test_10 USING value < 50 AS permissive TO ALL;

-- Direct query to underlying table
SELECT count() FROM row_policy_test_1;
-- ABR query (should route to row_policy_test_1 and apply the same policy)
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR;
-- They must match
SELECT
    (SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR) =
    (SELECT count() FROM row_policy_test_1) AS counts_match;

DROP ROW POLICY 03586_filter_value ON row_policy_test_1;
DROP ROW POLICY 03586_filter_value ON row_policy_test_10;

-- Test 3: Row policy filtering on a column NOT in SELECT list
-- Policy uses account_id but we only SELECT count()
-- This tests that extra columns needed by the policy are fetched
CREATE ROW POLICY 03586_filter_account ON row_policy_test_1 USING account_id = 'acct_A' AS permissive TO ALL;
CREATE ROW POLICY 03586_filter_account ON row_policy_test_10 USING account_id = 'acct_A' AS permissive TO ALL;

SELECT count() FROM row_policy_test_1;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR;
SELECT
    (SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR) =
    (SELECT count() FROM row_policy_test_1) AS counts_match;

-- Test 4: Row policy + WHERE clause combined
-- Policy filters to acct_A, WHERE further filters value < 25
SELECT count() FROM row_policy_test_1 WHERE value < 25;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR AND value < 25;
SELECT
    (SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR AND value < 25) =
    (SELECT count() FROM row_policy_test_1 WHERE value < 25) AS counts_match;

DROP ROW POLICY 03586_filter_account ON row_policy_test_1;
DROP ROW POLICY 03586_filter_account ON row_policy_test_10;

-- Test 5: After dropping policies, full data is visible again
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR;
SELECT count() FROM row_policy_test_1;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_policy_test_1;
DROP TABLE IF EXISTS row_policy_test_10;
