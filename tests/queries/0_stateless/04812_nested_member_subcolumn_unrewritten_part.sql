-- Reading only a dotted Nested member's subcolumn from a part that predates a MODIFY COLUMN used to
-- abort: the synthesized Nested group took its element type from the subcolumn entry (metadata type)
-- while the column was read with the part's type, so the type-directed `enumerateStreams` walk in
-- `collectOffsetsColumns` hit `Bad cast from type DB::ColumnString to DB::ColumnNullable`.
-- `min_bytes_for_wide_part`/`min_rows_for_wide_part` pin a Compact part (a Wide part is a control,
-- see below) and `share_nested_offsets` pins the code path; both are randomized by the test runner.

SELECT '-- witness: `.null` alone on an unrewritten Compact part';
DROP TABLE IF EXISTS t_nested_unrewritten;
CREATE TABLE t_nested_unrewritten (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_unrewritten VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_nested_unrewritten;
ALTER TABLE t_nested_unrewritten MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS alter_sync = 0;
SELECT `arr.n`.null FROM t_nested_unrewritten;

-- The trigger is the ORDER of the projection, not whether the parent is present: the first entry
-- for a group member decides its element type, and a subcolumn entry carries the metadata type.
SELECT '-- order matrix: subcolumn first, then parent';
SELECT `arr.n`.null, `arr.n` FROM t_nested_unrewritten;
SELECT '-- order matrix: parent first, then subcolumn';
SELECT `arr.n`, `arr.n`.null FROM t_nested_unrewritten;
SELECT '-- order matrix: subcolumn first, then an unrelated column';
SELECT `arr.n`.null, id FROM t_nested_unrewritten;
DROP TABLE t_nested_unrewritten;

SELECT '-- a declared Nested type, not the flattened spelling';
DROP TABLE IF EXISTS t_nested_real;
CREATE TABLE t_nested_real (id UInt8, arr Nested(n String, i UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_real VALUES (1, ['a', 'b'], [10, 20]);
SYSTEM STOP MERGES t_nested_real;
ALTER TABLE t_nested_real MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS alter_sync = 0;
SELECT `arr.n`.null FROM t_nested_real;
SELECT '-- a sibling member alongside the subcolumn';
SELECT `arr.n`.null, `arr.i` FROM t_nested_real;
DROP TABLE t_nested_real;

-- The divergence is not specific to Nullable: any wrapper the part lacks aborts at the first level
-- whose column class differs from the metadata type.
SELECT '-- a non-Nullable wrapper: Array(String) -> Array(Tuple(x String))';
DROP TABLE IF EXISTS t_nested_tuple;
CREATE TABLE t_nested_tuple (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_tuple VALUES (1, ['(\'p\')', '(\'q\')']);
SYSTEM STOP MERGES t_nested_tuple;
ALTER TABLE t_nested_tuple MODIFY COLUMN `arr.n` Array(Tuple(x String)) SETTINGS alter_sync = 0;
SELECT `arr.n`.x FROM t_nested_tuple;
DROP TABLE t_nested_tuple;

-- Controls. Each returns the same values as the witness and must be unaffected by the fix; they are
-- what makes the witness rows attributable to the part type rather than to an invalid fixture.
SELECT '-- control: a Wide part stores the member separately';
DROP TABLE IF EXISTS t_nested_wide;
CREATE TABLE t_nested_wide (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1;
INSERT INTO t_nested_wide VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_nested_wide;
ALTER TABLE t_nested_wide MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS alter_sync = 0;
SELECT `arr.n`.null FROM t_nested_wide;
DROP TABLE t_nested_wide;

SELECT '-- control: a rewritten part already carries the new type (ground truth)';
DROP TABLE IF EXISTS t_nested_rewritten;
CREATE TABLE t_nested_rewritten (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_rewritten VALUES (1, ['a', 'b']);
ALTER TABLE t_nested_rewritten MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS mutations_sync = 2;
SELECT `arr.n`.null FROM t_nested_rewritten;
DROP TABLE t_nested_rewritten;

SELECT '-- control: shared Nested offsets disabled, so the group is never synthesized';
DROP TABLE IF EXISTS t_nested_unshared;
CREATE TABLE t_nested_unshared (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 0;
INSERT INTO t_nested_unshared VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_nested_unshared;
ALTER TABLE t_nested_unshared MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS alter_sync = 0;
SELECT `arr.n`.null FROM t_nested_unshared;
DROP TABLE t_nested_unshared;

SELECT '-- control: a dotted column that is not an Array never joins a group';
DROP TABLE IF EXISTS t_nested_scalar;
CREATE TABLE t_nested_scalar (id UInt8, `grp.v` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1;
INSERT INTO t_nested_scalar VALUES (1, 7);
SYSTEM STOP MERGES t_nested_scalar;
ALTER TABLE t_nested_scalar MODIFY COLUMN `grp.v` Nullable(UInt8) SETTINGS alter_sync = 0;
SELECT `grp.v`.null FROM t_nested_scalar;
DROP TABLE t_nested_scalar;
