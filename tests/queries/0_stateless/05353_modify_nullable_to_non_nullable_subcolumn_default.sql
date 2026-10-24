-- While `MODIFY COLUMN` from `Nullable` to a non-`Nullable` type with a `DEFAULT` is pending, the NULL rows of a part
-- read as the `DEFAULT`; its subcolumns, and the functions rewritten to them, must read the same values.

SET enable_nullable_tuple_type = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_tuple_compact;
DROP TABLE IF EXISTS t_tuple_wide;
DROP TABLE IF EXISTS t_string_compact;
DROP TABLE IF EXISTS t_string_wide;
DROP TABLE IF EXISTS t_lc_compact;
DROP TABLE IF EXISTS t_lc_wide;
DROP TABLE IF EXISTS t_on_fly;
DROP TABLE IF EXISTS t_mutation;
DROP TABLE IF EXISTS t_remove_default;
DROP TABLE IF EXISTS t_sorting_key;
DROP TABLE IF EXISTS t_partition_key;
DROP TABLE IF EXISTS t_patch;

-- Tuple.

CREATE TABLE t_tuple_compact (c0 UInt64, x Nullable(Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))))
ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 10000000000;
CREATE TABLE t_tuple_wide (c0 UInt64, x Nullable(Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))))
ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_tuple_compact VALUES (1, NULL), (2, (10, 20, [1], 'zz', (30)));
INSERT INTO t_tuple_wide VALUES (1, NULL), (2, (10, 20, [1], 'zz', (30)));

SYSTEM STOP MERGES t_tuple_compact;
SYSTEM STOP MERGES t_tuple_wide;

ALTER TABLE t_tuple_compact MODIFY COLUMN x Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))
DEFAULT tuple(7, 8, [5, 6], 'def', tuple(9)) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_tuple_wide MODIFY COLUMN x Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))
DEFAULT tuple(7, 8, [5, 6], 'def', tuple(9)) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'tuple, compact';
SELECT c0, x.a, x.b, x.arr, x.arr.size0, x.s, x.s.size, x.n, x.n.d FROM t_tuple_compact ORDER BY c0;
SELECT c0, x, x.a FROM t_tuple_compact ORDER BY c0;
SELECT c0 FROM t_tuple_compact WHERE x.a = 7;
SELECT c0 FROM t_tuple_compact PREWHERE x.a = 7;
SELECT c0 FROM t_tuple_compact WHERE x.b = 8 AND x.a = 7
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, enable_multiple_prewhere_read_steps = 1;
SELECT c0, tupleElement(x, 'a') FROM t_tuple_compact ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, x.a FROM t_tuple_compact ORDER BY c0 LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT c0, x.a, x.n.d FROM t_tuple_compact PREWHERE toString(x) != '' ORDER BY c0;

SELECT 'tuple, wide';
SELECT c0, x.a, x.b, x.arr, x.arr.size0, x.s, x.s.size, x.n, x.n.d FROM t_tuple_wide ORDER BY c0;
SELECT c0, x, x.a FROM t_tuple_wide ORDER BY c0;
SELECT c0 FROM t_tuple_wide WHERE x.a = 7;
SELECT c0 FROM t_tuple_wide PREWHERE x.a = 7;
SELECT c0 FROM t_tuple_wide WHERE x.b = 8 AND x.a = 7
SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, enable_multiple_prewhere_read_steps = 1;
SELECT c0, tupleElement(x, 'a') FROM t_tuple_wide ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, x.a FROM t_tuple_wide ORDER BY c0 LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT c0, x.a, x.n.d FROM t_tuple_wide PREWHERE toString(x) != '' ORDER BY c0;

-- String.

CREATE TABLE t_string_compact (c0 UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 10000000000;
CREATE TABLE t_string_wide (c0 UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_string_compact VALUES (1, NULL), (2, 'hello');
INSERT INTO t_string_wide VALUES (1, NULL), (2, 'hello');

SYSTEM STOP MERGES t_string_compact;
SYSTEM STOP MERGES t_string_wide;

ALTER TABLE t_string_compact MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_string_wide MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'string, compact';
SELECT c0, s.size FROM t_string_compact ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_string_compact ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_string_compact WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'string, wide';
SELECT c0, s.size FROM t_string_wide ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_string_wide ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_string_wide WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;

-- LowCardinality(Nullable(String)).

CREATE TABLE t_lc_compact (c0 UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 10000000000;
CREATE TABLE t_lc_wide (c0 UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lc_compact VALUES (1, NULL), (2, 'hello');
INSERT INTO t_lc_wide VALUES (1, NULL), (2, 'hello');

SYSTEM STOP MERGES t_lc_compact;
SYSTEM STOP MERGES t_lc_wide;

ALTER TABLE t_lc_compact MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_lc_wide MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'low cardinality, compact';
SELECT c0, s.size FROM t_lc_compact ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_lc_compact ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_lc_compact WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'low cardinality, wide';
SELECT c0, s.size FROM t_lc_wide ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_lc_wide ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_lc_wide WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;

-- A pending UPDATE of another column applied on the fly.

CREATE TABLE t_on_fly (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64)), y UInt64 DEFAULT 0)
ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_on_fly (c0, x) VALUES (1, NULL), (2, (10, 20));
SYSTEM STOP MERGES t_on_fly;
ALTER TABLE t_on_fly UPDATE y = c0 * 100 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_on_fly MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'on the fly';
SELECT c0, x.a, y FROM t_on_fly ORDER BY c0 SETTINGS apply_mutations_on_fly = 1;

-- Mutations that reach a part before the conversion has rewritten it.

CREATE TABLE t_mutation (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64)), y UInt64 DEFAULT 0)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 10000000000;
INSERT INTO t_mutation (c0, x) VALUES (1, NULL), (2, (10, 20)), (3, (0, 0));
SYSTEM STOP MERGES t_mutation;
ALTER TABLE t_mutation MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_mutation UPDATE y = x.b WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_mutation DELETE WHERE x.a = 0 SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_mutation;
ALTER TABLE t_mutation UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutation';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation' AND NOT is_done;
SELECT c0, x, y FROM t_mutation ORDER BY c0;

-- Without a DEFAULT the parent read by PREWHERE must still be the source of the subcolumn.

CREATE TABLE t_remove_default (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY c0 SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_remove_default VALUES (1, NULL), (2, (10, 20));
SYSTEM STOP MERGES t_remove_default;
ALTER TABLE t_remove_default MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_remove_default MODIFY COLUMN x REMOVE DEFAULT SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'default removed';
SELECT c0, x, x.a FROM t_remove_default ORDER BY c0;
SELECT c0, x.a FROM t_remove_default PREWHERE toString(x) != '' ORDER BY c0;

-- A patch written before the ALTER: rows 1, 2 are in a part without the column, rows 3, 4 in a part with it.

CREATE TABLE t_patch (c0 UInt64) ENGINE = MergeTree ORDER BY c0
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, patch_parts_version = 'v2';
INSERT INTO t_patch VALUES (1), (2);
SYSTEM STOP MERGES t_patch;
ALTER TABLE t_patch ADD COLUMN s Nullable(String);
INSERT INTO t_patch VALUES (3, NULL), (4, 'x');
UPDATE t_patch SET s = 'updated' WHERE c0 IN (1, 3);
UPDATE t_patch SET s = NULL WHERE c0 = 4;
ALTER TABLE t_patch MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'patches';
SELECT c0, s, s.size, length(s) FROM t_patch ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT c0, s.size FROM t_patch ORDER BY c0;
SELECT arraySort(groupArray(c0)) FROM t_patch WHERE length(s) = 7 SETTINGS optimize_functions_to_subcolumns = 1;

-- Replacing the NULLs changes the values of key subcolumns, which a mutation does not re-sort or repartition.

CREATE TABLE t_sorting_key (c0 UInt64, x Nullable(Tuple(a Nullable(UInt64), b UInt64)))
ENGINE = MergeTree ORDER BY x.a SETTINGS allow_nullable_key = 1;
CREATE TABLE t_partition_key (c0 UInt64, x Nullable(Tuple(a Nullable(UInt64), b UInt64)))
ENGINE = MergeTree PARTITION BY x.a ORDER BY c0 SETTINGS allow_nullable_key = 1;
INSERT INTO t_sorting_key VALUES (1, NULL), (2, (10, 20));
INSERT INTO t_partition_key VALUES (1, NULL), (2, (10, 20));

ALTER TABLE t_sorting_key MODIFY COLUMN x Tuple(a Nullable(UInt64), b UInt64) DEFAULT tuple(7, 8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_partition_key MODIFY COLUMN x Tuple(a Nullable(UInt64), b UInt64) DEFAULT tuple(7, 8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_sorting_key MODIFY COLUMN x Nullable(Tuple(a Nullable(UInt64), b String)) SETTINGS mutations_sync = 2;

SELECT 'key subcolumns';
SELECT c0, x, x.a FROM t_sorting_key ORDER BY c0;

DROP TABLE t_tuple_compact;
DROP TABLE t_tuple_wide;
DROP TABLE t_string_compact;
DROP TABLE t_string_wide;
DROP TABLE t_lc_compact;
DROP TABLE t_lc_wide;
DROP TABLE t_on_fly;
DROP TABLE t_mutation;
DROP TABLE t_remove_default;
DROP TABLE t_sorting_key;
DROP TABLE t_partition_key;
DROP TABLE t_patch;
