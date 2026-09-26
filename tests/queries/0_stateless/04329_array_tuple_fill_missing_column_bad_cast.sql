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

-- A wrapper between the array and the tuple stops the type descent, so the part of the subcolumn
-- path below it is still unresolved when the default column is built.
DROP TABLE IF EXISTS t_add_wrapped_memory;
CREATE TABLE t_add_wrapped_memory (`n.a` Array(Tuple(aa UInt64))) ENGINE = Memory;
INSERT INTO t_add_wrapped_memory SELECT [tuple(1), tuple(2), tuple(3)];

ALTER TABLE t_add_wrapped_memory ADD COLUMN `n.b` Array(Nullable(Tuple(bb UInt64)))
SETTINGS enable_nullable_tuple_type = 1;
SELECT materialize(`n.b`.bb), `n.a` FROM t_add_wrapped_memory;

-- An unresolved path can add an array dimension of its own, which the offsets must not add again.
ALTER TABLE t_add_wrapped_memory ADD COLUMN `n.c` Array(Nullable(Tuple(k Map(String, UInt64), z UInt8)))
SETTINGS enable_nullable_tuple_type = 1;
SELECT materialize(`n.c`.k.keys), materialize(`n.c`.k), materialize(`n.c`.z), `n.a` FROM t_add_wrapped_memory;

-- A wrapper below a tuple element leaves the element resolved and the path under it unresolved, so
-- the descent answers only part of the path.
ALTER TABLE t_add_wrapped_memory ADD COLUMN `n.d` Array(Tuple(k Nullable(Tuple(bb UInt64)), z UInt8))
SETTINGS enable_nullable_tuple_type = 1;
SELECT materialize(`n.d`.k.bb), `n.a` FROM t_add_wrapped_memory;

-- Paths of the same column that the descent resolves completely, as contrast.
SELECT materialize(`n.d`.k), materialize(`n.d`.z), `n.a` FROM t_add_wrapped_memory;

-- The same partly resolved path, where the unresolved part also adds an array dimension.
ALTER TABLE t_add_wrapped_memory ADD COLUMN `n.e` Array(Tuple(k Nullable(Tuple(m Map(String, UInt64))), z UInt8))
SETTINGS enable_nullable_tuple_type = 1;
SELECT materialize(`n.e`.k.m.keys), `n.a` FROM t_add_wrapped_memory;
DROP TABLE t_add_wrapped_memory;
