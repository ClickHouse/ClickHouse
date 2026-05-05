-- Tags: no-parallel
-- Other tests can enable failpoints and interfere with this test

-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- All four types are present
SELECT type, count() > 0 FROM system.fail_points GROUP BY type ORDER BY type;
-- Expected:
-- once            1
-- regular         1
-- pauseable_once  1
-- pauseable       1

-- Filtering by type works
SELECT count() > 0 FROM system.fail_points WHERE type = 'pauseable';

-- Filtering by name with LIKE
SELECT count() > 0 FROM system.fail_points WHERE name LIKE '%smt_%';
