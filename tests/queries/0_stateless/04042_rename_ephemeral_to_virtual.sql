-- Tags: no-fasttest
-- PR #99549 added validation that `EPHEMERAL` columns cannot be named the same
-- as virtual columns. The PR's own test (`04040`) covers `CREATE TABLE` and
-- `ADD COLUMN`/`MODIFY COLUMN`; the `RENAME COLUMN` path at
-- `AlterCommands.cpp:1749-1753` was not covered.

DROP TABLE IF EXISTS t_04042_re;
CREATE TABLE t_04042_re (key UInt32, eph UInt8 EPHEMERAL 0) ENGINE = MergeTree ORDER BY key;

-- Renaming an EPHEMERAL column to a virtual column name must be rejected
ALTER TABLE t_04042_re RENAME COLUMN eph TO `_part_offset`; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_04042_re RENAME COLUMN eph TO `_part`; -- { serverError ILLEGAL_COLUMN }

-- Renaming a non-EPHEMERAL column to a virtual column name is still allowed
-- (DEFAULT columns have physical storage and can shadow virtual columns)
ALTER TABLE t_04042_re ADD COLUMN def UInt64 DEFAULT 0;
ALTER TABLE t_04042_re RENAME COLUMN def TO `_part_offset`;
ALTER TABLE t_04042_re RENAME COLUMN `_part_offset` TO def;
ALTER TABLE t_04042_re DROP COLUMN def;

DROP TABLE t_04042_re;
