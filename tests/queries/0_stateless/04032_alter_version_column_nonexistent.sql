-- Regression test: special MergeTree columns (version, sign, is_deleted) must not
-- be changed to EPHEMERAL or ALIAS. Previously, making the version column EPHEMERAL
-- removed it from physical columns, causing a null pointer dereference (UBSan) on
-- subsequent ALTER MODIFY COLUMN.

SET allow_suspicious_low_cardinality_types = 1;

-- Test version column (ReplacingMergeTree)
DROP TABLE IF EXISTS t_special_col;
CREATE TABLE t_special_col (a UInt64, b DateTime) ENGINE = ReplacingMergeTree(b) ORDER BY a;
ALTER TABLE t_special_col MODIFY COLUMN b DateTime EPHEMERAL now(); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_special_col MODIFY COLUMN b DateTime ALIAS now(); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_special_col;

-- Test sign column (CollapsingMergeTree)
CREATE TABLE t_special_col (a UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY a;
ALTER TABLE t_special_col MODIFY COLUMN sign Int8 EPHEMERAL 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_special_col MODIFY COLUMN sign Int8 ALIAS 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_special_col;

-- Test is_deleted column (ReplacingMergeTree with is_deleted)
CREATE TABLE t_special_col (a UInt64, b UInt64, deleted UInt8) ENGINE = ReplacingMergeTree(b, deleted) ORDER BY a;
ALTER TABLE t_special_col MODIFY COLUMN deleted UInt8 EPHEMERAL 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_special_col MODIFY COLUMN deleted UInt8 ALIAS 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_special_col;
