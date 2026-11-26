-- Test: RENAME COLUMN should reject renaming an EPHEMERAL column to a virtual column name
-- This tests the validation in AlterCommands.cpp for the RENAME path

DROP TABLE IF EXISTS t_rename_eph;

CREATE TABLE t_rename_eph (key UInt32, x UInt8 EPHEMERAL 0) ENGINE = MergeTree ORDER BY key;

-- Renaming an ephemeral column to a virtual column name should fail
ALTER TABLE t_rename_eph RENAME COLUMN x TO _part_offset; -- { serverError ILLEGAL_COLUMN }

DROP TABLE IF EXISTS t_rename_eph;
