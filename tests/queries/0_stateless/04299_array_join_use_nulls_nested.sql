-- Regression test for array_join_use_nulls with Nested columns and subcolumn pruning.
--
-- A LEFT ARRAY JOIN over a Nested column produces a nested() expression. When only some
-- subcolumns are referenced, PruneArrayJoinColumnsPass prunes the unused ones. The pruning
-- must NOT drop the array_join_use_nulls nullable contract: empty nested arrays must still
-- yield NULL, and the result type must stay Nullable for every access form.

DROP TABLE IF EXISTS t_04299;
CREATE TABLE t_04299 (n Nested(x UInt8, y UInt8)) ENGINE = Memory;
INSERT INTO t_04299 VALUES ([1], [2]);
INSERT INTO t_04299 VALUES ([], []);

SET array_join_use_nulls = 1;

-- Only n.x referenced via tupleElement: pruning of n.y must not drop the nullable.
SELECT 'tupleElement only x';
SELECT tupleElement(n, 'x') AS v, toTypeName(v), v IS NULL FROM t_04299 LEFT ARRAY JOIN n ORDER BY v;

-- Subcolumn syntax n.x: lowers to getSubcolumn (no pruning), but the nullable must still hold.
SELECT 'subcolumn n.x';
SELECT n.x AS v, toTypeName(v), v IS NULL FROM t_04299 LEFT ARRAY JOIN n ORDER BY v;

-- Only n.y referenced: pruning of n.x must not drop the nullable.
SELECT 'tupleElement only y';
SELECT tupleElement(n, 'y') AS v, toTypeName(v), v IS NULL FROM t_04299 LEFT ARRAY JOIN n ORDER BY v;

-- Numeric tupleElement index that is remapped by pruning (only the 2nd subcolumn used: x pruned, 2 -> 1).
SELECT 'tupleElement numeric index 2';
SELECT tupleElement(n, 2) AS v, toTypeName(v), v IS NULL FROM t_04299 LEFT ARRAY JOIN n ORDER BY v;

-- Both subcolumns (no pruning).
SELECT 'both subcolumns';
SELECT n.x AS vx, n.y AS vy, toTypeName(vx) FROM t_04299 LEFT ARRAY JOIN n ORDER BY vx, vy;

-- The pruned nullable type must propagate through a wrapping query and CREATE AS SELECT.
SELECT 'create as select stored type';
DROP TABLE IF EXISTS dst_04299;
CREATE TABLE dst_04299 ENGINE = Memory AS SELECT tupleElement(n, 'x') AS v FROM t_04299 LEFT ARRAY JOIN n;
SELECT type FROM system.columns WHERE table = 'dst_04299' AND database = currentDatabase();
SELECT v IS NULL FROM dst_04299 ORDER BY v;

-- Two Nested columns array-joined together, a subcolumn pruned in each: both stay nullable.
SELECT 'two nested columns';
DROP TABLE IF EXISTS t2_04299;
CREATE TABLE t2_04299 (na Nested(x UInt8, y UInt8), nb Nested(p UInt8, q UInt8)) ENGINE = Memory;
INSERT INTO t2_04299 VALUES ([1], [2], [3], [4]);
INSERT INTO t2_04299 VALUES ([], [], [], []);
SELECT tupleElement(na, 'x') AS vx, tupleElement(nb, 'q') AS vq, toTypeName(vx), toTypeName(vq), vx IS NULL
FROM t2_04299 LEFT ARRAY JOIN na, nb ORDER BY vx;

-- SELECT * must expand the Nested array-join column into per-row Nullable scalars, NOT leak the
-- original table arrays. (Regression: the matcher skipped the Nested expansion for Nullable(Tuple).)
SELECT 'select star';
SELECT *, toTypeName(n.x) FROM t_04299 LEFT ARRAY JOIN n ORDER BY n.x;

-- array_join_use_nulls = 0: behavior unchanged (non-nullable, empty arrays produce defaults).
SET array_join_use_nulls = 0;
SELECT 'array_join_use_nulls = 0';
SELECT tupleElement(n, 'x') AS v, toTypeName(v) FROM t_04299 LEFT ARRAY JOIN n ORDER BY v;
SELECT 'select star use_nulls=0';
SELECT *, toTypeName(n.x) FROM t_04299 LEFT ARRAY JOIN n ORDER BY n.x;

DROP TABLE dst_04299;
DROP TABLE t2_04299;
DROP TABLE t_04299;
