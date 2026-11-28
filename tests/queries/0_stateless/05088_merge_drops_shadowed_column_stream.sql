-- A column that no source part holds and that has no default expression is dropped from the merged
-- part. Its files were looked up through the part's serialization of that name, which - once the
-- column itself is gone from the part - is the serialization of a same-named subcolumn of another
-- column (`a.size0` of an `Array` column `a`). The removal then deleted that other column's stream:
-- the array lost its offsets file and every read of the table failed with a logical error. (What that
-- dropped column itself reads is a separate matter, see issue #114588.)

DROP TABLE IF EXISTS t_merge_drops_offsets;
CREATE TABLE t_merge_drops_offsets (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_merge_drops_offsets VALUES ([10, 20, 30]), ([40]);
ALTER TABLE t_merge_drops_offsets ADD COLUMN `a.size0` UInt64;
OPTIMIZE TABLE t_merge_drops_offsets FINAL;

SELECT 'the array survives the merge';
SELECT a FROM t_merge_drops_offsets ORDER BY a;
SELECT sum(length(a)) FROM t_merge_drops_offsets;

-- The same for a `Nested` column, whose offsets are shared by its elements.
DROP TABLE IF EXISTS t_merge_drops_nested_offsets;
CREATE TABLE t_merge_drops_nested_offsets (n Nested(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_merge_drops_nested_offsets VALUES ([1, 2], [3, 4]);
ALTER TABLE t_merge_drops_nested_offsets ADD COLUMN `n.size0` UInt64;
OPTIMIZE TABLE t_merge_drops_nested_offsets FINAL;

SELECT 'the nested column survives the merge';
SELECT n.x, n.y FROM t_merge_drops_nested_offsets;
SELECT length(n.x), length(n.y) FROM t_merge_drops_nested_offsets;

DROP TABLE t_merge_drops_nested_offsets;
DROP TABLE t_merge_drops_offsets;
