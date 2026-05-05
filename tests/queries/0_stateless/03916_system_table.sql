-- Tags: no-parallel

-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- All four types are present
SELECT type, count() > 0 FROM system.fail_points GROUP BY type ORDER BY type;

-- Filtering by type works
SELECT count() > 0 FROM system.fail_points WHERE type = 'pauseable';

-- Filtering by name with LIKE
SELECT count() > 0 FROM system.fail_points WHERE name LIKE '%smt_%';
