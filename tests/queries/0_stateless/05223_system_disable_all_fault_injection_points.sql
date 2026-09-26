-- Tags: no-parallel
-- no-parallel: fail points are server-wide, and this test disables all of them.

-- `SYSTEM DISABLE ALL FAILPOINTS` disables every fail point at once, so a test harness can
-- hand the next test a server that injects nothing without knowing what the previous test
-- armed - see https://github.com/ClickHouse/ClickHouse/issues/118832.

-- Not `WHERE enabled` over the whole table: another test may leave a fail point armed, which
-- is exactly the situation this statement exists for, and would make the counts flaky. Two
-- names are used, one `regular` and one `pauseable`, because they are disabled differently -
-- a pauseable one also has to have its wait channel dropped.
SYSTEM ENABLE FAILPOINT dummy_failpoint;
SYSTEM ENABLE FAILPOINT dummy_pausable_failpoint;

SELECT name, enabled FROM system.fail_points
WHERE name IN ('dummy_failpoint', 'dummy_pausable_failpoint') ORDER BY name;

SYSTEM DISABLE ALL FAILPOINTS;

SELECT name, enabled FROM system.fail_points
WHERE name IN ('dummy_failpoint', 'dummy_pausable_failpoint') ORDER BY name;

-- Idempotent: disabling nothing is not an error.
SYSTEM DISABLE ALL FAILPOINTS;

-- Takes no argument, and no `ON CLUSTER` - like every other `SYSTEM ... FAILPOINT` statement.
SELECT formatQuerySingleLine('SYSTEM DISABLE ALL FAILPOINTS');
SELECT formatQuerySingleLine('SYSTEM DISABLE ALL FAILPOINTS dummy_failpoint'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('SYSTEM DISABLE ALL FAILPOINTS ON CLUSTER test_shard_localhost'); -- { serverError SYNTAX_ERROR }
