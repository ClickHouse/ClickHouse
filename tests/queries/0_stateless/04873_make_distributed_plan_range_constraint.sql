-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: creates a server-global settings profile under a fixed name.

-- A range constraint that forbids the enabled value must veto the `make_distributed_plan`
-- derivation exactly like a `const` pin does. Where the environment already pins the setting
-- const for every user (ClickHouse Cloud does), `CREATE SETTINGS PROFILE` below fails with
-- `should not be changed`, which the Cloud test harness treats as a skip, so the expectations
-- assume no ambient pin.

DROP SETTINGS PROFILE IF EXISTS profile_04873;
CREATE SETTINGS PROFILE profile_04873 SETTINGS make_distributed_plan MAX 0;

SET profile = 'profile_04873';

SELECT 'not derived under the range constraint';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan');

SET make_distributed_plan = 1; -- { serverError SETTING_CONSTRAINT_VIOLATION }

DROP SETTINGS PROFILE profile_04873;
