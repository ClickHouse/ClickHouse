-- A column added by `ALTER TABLE ... ADD COLUMN` whose name is also the name of a generated
-- subcolumn of another column (`a.size0` next to an `Array` column `a`) is missing from the parts
-- written before the ALTER. Those parts used to answer that name with the subcolumn - the array's
-- sizes - instead of the column's default, so one `SELECT` mixed two meanings of the same identifier
-- and a merge stored the wrong values permanently.

DROP TABLE IF EXISTS t_added_size0;
CREATE TABLE t_added_size0 (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_added_size0 VALUES ([0]), ([1, 2]);
ALTER TABLE t_added_size0 ADD COLUMN `a.size0` UInt64 DEFAULT 7;
INSERT INTO t_added_size0 (a, `a.size0`) VALUES ([3], 7);

SELECT 'an explicit default in the parts that predate the column';
SELECT a, `a.size0` FROM t_added_size0 ORDER BY a;

SELECT 'and after a merge stores it';
OPTIMIZE TABLE t_added_size0 FINAL;
SELECT a, `a.size0` FROM t_added_size0 ORDER BY a;

SELECT 'the array is still the array';
SELECT sum(length(a)) FROM t_added_size0;

DROP TABLE t_added_size0;

DROP TABLE IF EXISTS t_added_size0_implicit;
CREATE TABLE t_added_size0_implicit (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
-- A merge of a column without a default expression drops it from the merged part, which is a
-- separate defect for this name collision: it takes the array's offsets file with it. Keep the parts
-- as they are, so this test covers the read of the column instead.
SYSTEM STOP MERGES t_added_size0_implicit;
INSERT INTO t_added_size0_implicit VALUES ([10, 20, 30]);
ALTER TABLE t_added_size0_implicit ADD COLUMN `a.size0` UInt64;

SELECT 'the implicit default of the type';
SELECT a, `a.size0` FROM t_added_size0_implicit;

DROP TABLE t_added_size0_implicit;

-- A `Nested` column is read through the same lookup: its flattened columns are collected back into
-- one column of the part and read as its subcolumns, which must keep working.
DROP TABLE IF EXISTS t_nested_after_add;
CREATE TABLE t_nested_after_add (id UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested_after_add VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);
ALTER TABLE t_nested_after_add ADD COLUMN z UInt64 DEFAULT 5;
INSERT INTO t_nested_after_add VALUES (3, [4], ['d'], 9);

SELECT 'a nested column and a column added after it';
SELECT id, n.x, n.y, z FROM t_nested_after_add ORDER BY id;
SELECT id, length(n.x) FROM t_nested_after_add ORDER BY id;

DROP TABLE t_nested_after_add;

-- The same without the shared offsets of `Nested`, which changes the shape of the read request.
DROP TABLE IF EXISTS t_nested_own_offsets;
CREATE TABLE t_nested_own_offsets (id UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 0;
INSERT INTO t_nested_own_offsets VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);
ALTER TABLE t_nested_own_offsets ADD COLUMN z UInt64 DEFAULT 5;
INSERT INTO t_nested_own_offsets VALUES (3, [4], ['d'], 9);

SELECT 'a nested column with its own offsets';
SELECT id, n.x, n.y, z FROM t_nested_own_offsets ORDER BY id;
SELECT id, length(n.x) FROM t_nested_own_offsets ORDER BY id;

DROP TABLE t_nested_own_offsets;
