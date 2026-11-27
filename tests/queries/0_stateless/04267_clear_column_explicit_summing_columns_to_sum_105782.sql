-- Tags: no-random-merge-tree-settings

SET mutations_sync = 2;
SET alter_sync = 2;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_clear_column_explicit_sum_105782;

CREATE TABLE t_clear_column_explicit_sum_105782
(
    p UInt32,
    v UInt32,
    c Int64
)
ENGINE = SummingMergeTree(c)
PARTITION BY p
ORDER BY v;

SYSTEM STOP MERGES t_clear_column_explicit_sum_105782;
INSERT INTO t_clear_column_explicit_sum_105782 VALUES (1, 1, 5), (1, 2, 10);
INSERT INTO t_clear_column_explicit_sum_105782 VALUES (1, 1, 7), (1, 2, 11);
SYSTEM START MERGES t_clear_column_explicit_sum_105782;

ALTER TABLE t_clear_column_explicit_sum_105782 CLEAR COLUMN c IN PARTITION 1;
OPTIMIZE TABLE t_clear_column_explicit_sum_105782 PARTITION 1 FINAL;

SELECT 'summing_rows_after_clear', count() FROM t_clear_column_explicit_sum_105782;
SELECT 'summing_column_meta_after_clear', name, type
FROM system.columns
WHERE database = currentDatabase()
  AND table = 't_clear_column_explicit_sum_105782'
  AND name = 'c';

ALTER TABLE t_clear_column_explicit_sum_105782 DROP COLUMN c; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_clear_column_explicit_sum_105782 RENAME COLUMN c TO c_renamed; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_column_explicit_sum_105782;

DROP TABLE IF EXISTS t_clear_column_explicit_coalescing_105782;

CREATE TABLE t_clear_column_explicit_coalescing_105782
(
    k UInt32,
    c UInt64
)
ENGINE = CoalescingMergeTree(c)
ORDER BY k;

INSERT INTO t_clear_column_explicit_coalescing_105782 VALUES (1, 10);
INSERT INTO t_clear_column_explicit_coalescing_105782 VALUES (1, 20);

ALTER TABLE t_clear_column_explicit_coalescing_105782 CLEAR COLUMN c;
OPTIMIZE TABLE t_clear_column_explicit_coalescing_105782 FINAL;

SELECT 'coalescing_clear_values', k, c FROM t_clear_column_explicit_coalescing_105782 ORDER BY k;
SELECT 'coalescing_column_meta_after_clear', name, type
FROM system.columns
WHERE database = currentDatabase()
  AND table = 't_clear_column_explicit_coalescing_105782'
  AND name = 'c';

ALTER TABLE t_clear_column_explicit_coalescing_105782 DROP COLUMN c; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_clear_column_explicit_coalescing_105782 RENAME COLUMN c TO c_renamed; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_column_explicit_coalescing_105782;

DROP TABLE IF EXISTS t_clear_column_special_replacing_105782;

CREATE TABLE t_clear_column_special_replacing_105782
(
    k UInt32,
    ver UInt64,
    is_deleted UInt8,
    v UInt64
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY k;

ALTER TABLE t_clear_column_special_replacing_105782 CLEAR COLUMN ver; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_clear_column_special_replacing_105782 CLEAR COLUMN is_deleted; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_column_special_replacing_105782;

DROP TABLE IF EXISTS t_clear_column_special_collapsing_105782;

CREATE TABLE t_clear_column_special_collapsing_105782
(
    k UInt32,
    sign Int8,
    v UInt64
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY k;

ALTER TABLE t_clear_column_special_collapsing_105782 CLEAR COLUMN sign; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_clear_column_special_collapsing_105782;
