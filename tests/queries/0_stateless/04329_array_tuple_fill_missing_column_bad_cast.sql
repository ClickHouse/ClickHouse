-- Tags: no-random-merge-tree-settings

-- ADD COLUMN: missing Array(Tuple(...)) column borrows offsets from a sibling Nested array.
DROP TABLE IF EXISTS t_add_memory;
CREATE TABLE t_add_memory (`n.a` Array(Tuple(aa UInt64))) ENGINE = Memory;
INSERT INTO t_add_memory SELECT [tuple(1), tuple(2), tuple(3)];
ALTER TABLE t_add_memory ADD COLUMN `n.b` Array(Tuple(bb UInt64));
SELECT * FROM t_add_memory;

ALTER TABLE t_add_memory ADD COLUMN `n.c` Array(Tuple(p Int, q String));
SELECT * FROM t_add_memory;
DROP TABLE t_add_memory;

-- CLEAR COLUMN: an unfinished mutation makes the column missing and applied on the fly.
DROP TABLE IF EXISTS t_clear_mt;
CREATE TABLE t_clear_mt (c0 Int, c1 Array(Tuple(c2 Int)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS apply_mutations_on_fly = 1;
SYSTEM STOP MERGES t_clear_mt;
INSERT INTO t_clear_mt (c0, c1) VALUES (1, [tuple(5)]);
ALTER TABLE t_clear_mt CLEAR COLUMN c1 SETTINGS alter_sync = 0;
INSERT INTO t_clear_mt (c0, c1) VALUES (2, []);
SELECT * FROM t_clear_mt ORDER BY c0;
DROP TABLE t_clear_mt;

-- A subcolumn whose own value type is a Tuple must keep its Tuple wrapper.
DROP TABLE IF EXISTS t_subcolumn_mt;
CREATE TABLE t_subcolumn_mt (c0 Int, c1 Array(Tuple(x Tuple(a Int, b Int), y Int)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS apply_mutations_on_fly = 1;
SYSTEM STOP MERGES t_subcolumn_mt;
INSERT INTO t_subcolumn_mt VALUES (1, [tuple(tuple(5, 6), 7)]);
ALTER TABLE t_subcolumn_mt CLEAR COLUMN c1 SETTINGS alter_sync = 0;
SELECT c1.x, c1.y FROM t_subcolumn_mt FORMAT JSONEachRow;
DROP TABLE t_subcolumn_mt;
