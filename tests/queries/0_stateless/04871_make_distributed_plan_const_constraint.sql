-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: creates a server-global settings profile under a fixed name.

-- A `const` constraint on make_distributed_plan must veto the value derived from
-- distributed_plan_workers_num, not just explicit changes.

DROP SETTINGS PROFILE IF EXISTS profile_04871;
CREATE SETTINGS PROFILE profile_04871 SETTINGS make_distributed_plan CONST;

SELECT 'derived while not pinned const';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan') = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan');

SELECT 'dropped when the const profile arrives';
SET profile = 'profile_04871';
SELECT getSetting('make_distributed_plan');

SELECT 'not derived under the constraint';
SET distributed_plan_workers_num = 5;
SELECT getSetting('make_distributed_plan');

SET make_distributed_plan = 1; -- { serverError SETTING_CONSTRAINT_VIOLATION }

DROP SETTINGS PROFILE profile_04871;
