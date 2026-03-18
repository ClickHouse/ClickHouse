-- ALTER TABLE should not allow creating EPHEMERAL columns that conflict with virtual columns.
-- This is the ALTER counterpart of the CREATE TABLE validation added in PR #99031.
-- https://github.com/ClickHouse/ClickHouse/issues/99437

DROP TABLE IF EXISTS t;

CREATE TABLE t (key UInt32) ENGINE = MergeTree ORDER BY key;

ALTER TABLE t ADD COLUMN `_part_offset` UInt8 EPHEMERAL 0; -- { serverError ILLEGAL_COLUMN }

-- MODIFY COLUMN to EPHEMERAL should also be rejected
ALTER TABLE t ADD COLUMN `_part_offset` UInt64;
ALTER TABLE t MODIFY COLUMN `_part_offset` UInt8 EPHEMERAL 0; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t DROP COLUMN `_part_offset`;

-- DEFAULT columns with virtual column names should still work (they have physical storage)
ALTER TABLE t ADD COLUMN `_part_offset` UInt64 DEFAULT 0;
ALTER TABLE t DROP COLUMN `_part_offset`;

DROP TABLE t;
