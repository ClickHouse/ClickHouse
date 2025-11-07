-- Test: Alias tables should be automatically dropped when their target table is dropped

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

-- Create a base table
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();

CREATE TABLE t1 ENGINE = Alias(t0);

-- Verify all three tables exist
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t0', 't1') ORDER BY name;

-- Drop the target table - this should automatically drop both alias tables
DROP TABLE t0 SYNC;

-- Verify that all tables (including alias tables) have been automatically dropped
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t0', 't1') ORDER BY name;

-- Clean up (should be no-op since tables were already dropped)
DROP TABLE IF EXISTS t1;
