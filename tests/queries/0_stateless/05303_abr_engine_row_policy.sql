-- Test: ABR engine row policy enforcement on source tables
-- Verifies that row policies defined on source tables are correctly
-- applied when querying through the ABR engine.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_alias;
DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_policy_test_1;
DROP TABLE IF EXISTS row_policy_test_10;
DROP ROW POLICY IF EXISTS 03586_filter_value ON row_policy_test_1;
DROP ROW POLICY IF EXISTS 03586_filter_value ON row_policy_test_10;
DROP ROW POLICY IF EXISTS 03586_filter_account ON row_policy_test_1;
DROP ROW POLICY IF EXISTS 03586_filter_account ON row_policy_test_10;
DROP ROW POLICY IF EXISTS 03586_abr_filter ON abr;
DROP ROW POLICY IF EXISTS 03586_abr_always_true ON abr;
DROP ROW POLICY IF EXISTS 03586_abr_other_users ON abr;
DROP ROW POLICY IF EXISTS 03586_abr_current_user_always_true ON abr;
DROP ROW POLICY IF EXISTS 03586_alias_filter ON abr_alias;
DROP ROW POLICY IF EXISTS 03586_alias_always_true ON abr_alias;

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
-- Applied to BOTH source tables
CREATE ROW POLICY 03586_filter_value ON row_policy_test_1 USING value < 50 AS permissive TO ALL;
CREATE ROW POLICY 03586_filter_value ON row_policy_test_10 USING value < 50 AS permissive TO ALL;

-- Direct query to source table
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

-- Test 6: A row policy on the ABR table itself is rejected, as it cannot be applied
-- to the query executed on the source table
CREATE ROW POLICY 03586_abr_filter ON abr USING account_id = 'acct_A' AS permissive TO ALL;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0; -- { serverError ACCESS_DENIED }
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1; -- { serverError ACCESS_DENIED }
DROP ROW POLICY 03586_abr_filter ON abr;

-- Test 7: An always-true row policy on the ABR table filters nothing, so it is not rejected
CREATE ROW POLICY 03586_abr_always_true ON abr USING 1 AS permissive TO ALL;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1;
DROP ROW POLICY 03586_abr_always_true ON abr;

-- Test 8: A row policy on the ABR table granted to other users does not affect the current user.
-- The current user gets an always-true policy, so that the read does not depend on the server setting
-- `access_control_improvements.throw_on_unmatched_row_policies`. No role is created: roles are shared by
-- all the tests running on the server, while row policies belong to a table of the test's database.
CREATE ROW POLICY 03586_abr_other_users ON abr USING account_id = 'acct_A' AS permissive TO ALL EXCEPT CURRENT_USER;
CREATE ROW POLICY 03586_abr_current_user_always_true ON abr USING 1 AS permissive TO CURRENT_USER;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1;
DROP ROW POLICY 03586_abr_other_users ON abr;
DROP ROW POLICY 03586_abr_current_user_always_true ON abr;

-- Test 9: The row policy of a table that reads the ABR table, such as an `Alias`, reaches `ABR` as the row-level filter
-- of the read, which cannot be applied to the query executed on the source table either, so it is rejected too.
CREATE TABLE abr_alias ENGINE = Alias('abr');
CREATE ROW POLICY 03586_alias_filter ON abr_alias USING account_id = 'acct_A' AS permissive TO ALL;
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0; -- { serverError ACCESS_DENIED }
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1; -- { serverError ACCESS_DENIED }
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0, optimize_move_to_prewhere = 0; -- { serverError ACCESS_DENIED }
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0; -- { serverError ACCESS_DENIED }
DROP ROW POLICY 03586_alias_filter ON abr_alias;

-- An always-true policy on the alias filters nothing, so it is not rejected.
CREATE ROW POLICY 03586_alias_always_true ON abr_alias USING 1 AS permissive TO ALL;
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0;
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1;
DROP ROW POLICY 03586_alias_always_true ON abr_alias;

-- Policies on the source tables still apply through the alias.
CREATE ROW POLICY 03586_filter_account ON row_policy_test_1 USING account_id = 'acct_A' AS permissive TO ALL;
CREATE ROW POLICY 03586_filter_account ON row_policy_test_10 USING account_id = 'acct_A' AS permissive TO ALL;
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 0;
SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 99 HOUR SETTINGS enable_analyzer = 1;
DROP ROW POLICY 03586_filter_account ON row_policy_test_1;
DROP ROW POLICY 03586_filter_account ON row_policy_test_10;
DROP TABLE abr_alias;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_policy_test_1;
DROP TABLE IF EXISTS row_policy_test_10;
