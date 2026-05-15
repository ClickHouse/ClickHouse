-- Verify that EXPLAIN PLAN defaults to actions=1, compact=1, pretty=1 from 26.5,
-- and that the new umbrella setting `query_plan_pretty_default` + the `compatibility`
-- setting can roll the defaults back. `pretty=1` is detected via line-drawing
-- characters in the output, which neither old default nor `pretty=0` produces.
-- The `compatibility` check runs first because `applyCompatibilitySetting` only
-- touches settings the user has not explicitly set.

-- 1. compatibility = '25.5' must roll the default back (verifies SettingsChangesHistory wiring).
SET compatibility = '25.5';
SELECT countIf(explain LIKE '%├──%' OR explain LIKE '%└──%') = 0 AS no_pretty_under_compat
FROM (EXPLAIN PLAN SELECT number FROM numbers(10) WHERE number > 3);
SET compatibility = '';

-- 2. With default server settings: output should be pretty (contain box-drawing chars).
SELECT countIf(explain LIKE '%├──%' OR explain LIKE '%└──%') > 0 AS pretty_by_default
FROM (EXPLAIN PLAN SELECT number FROM numbers(10) WHERE number > 3);

-- 3. Disable the umbrella setting: output should NOT contain box-drawing chars.
SET query_plan_pretty_default = 0;
SELECT countIf(explain LIKE '%├──%' OR explain LIKE '%└──%') = 0 AS no_pretty_when_disabled
FROM (EXPLAIN PLAN SELECT number FROM numbers(10) WHERE number > 3);

-- 4. With the umbrella off, per-query SETTINGS still enable pretty output.
SELECT countIf(explain LIKE '%├──%' OR explain LIKE '%└──%') > 0 AS per_query_enables_pretty
FROM (EXPLAIN PLAN actions = 1, compact = 1, pretty = 1 SELECT number FROM numbers(10) WHERE number > 3);

-- 5. With the umbrella on, per-query SETTINGS still disable pretty output.
SET query_plan_pretty_default = 1;
SELECT countIf(explain LIKE '%├──%' OR explain LIKE '%└──%') = 0 AS per_query_disables_pretty
FROM (EXPLAIN PLAN actions = 0, compact = 0, pretty = 0 SELECT number FROM numbers(10) WHERE number > 3);
