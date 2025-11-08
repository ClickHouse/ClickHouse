-- Test: Alias tables with broken targets should not crash system.tables

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

-- Create a base table
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();

CREATE TABLE t1 ENGINE = Alias(t0);

-- Verify both tables exist
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t0', 't1') ORDER BY name;

-- Drop the target table - the alias table should remain but be broken
DROP TABLE t0 SYNC;

-- Verify that system.tables can still be queried even with a broken alias
-- The alias table should still be listed
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('t0', 't1') ORDER BY name;

-- Verify we can query system.tables with all columns even with broken alias
SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name = 't1';

-- Clean up the broken alias table
DROP TABLE IF EXISTS t1;
