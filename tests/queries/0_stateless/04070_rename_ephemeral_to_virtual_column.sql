-- Test: RENAME COLUMN should reject renaming an EPHEMERAL column to a virtual column name
-- This tests the validation in AlterCommands.cpp for the RENAME path

DROP TABLE IF EXISTS t_rename_eph;

CREATE TABLE t_rename_eph (key UInt32, x UInt8 EPHEMERAL 0) ENGINE = MergeTree ORDER BY key;

-- Renaming an ephemeral column to a virtual column name should fail
ALTER TABLE t_rename_eph RENAME COLUMN x TO _part_offset; -- { serverError ILLEGAL_COLUMN }

-- Renaming a non-ephemeral column to a virtual column name should work (DEFAULT has physical storage)
DROP TABLE IF EXISTS t_rename_def;
CREATE TABLE t_rename_def (key UInt32, x UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_rename_def RENAME COLUMN x TO _part_offset;
INSERT INTO t_rename_def (key) SELECT number FROM numbers(3);
SELECT key, _part_offset FROM t_rename_def ORDER BY key;

DROP TABLE IF EXISTS t_rename_eph;
DROP TABLE IF EXISTS t_rename_def;
