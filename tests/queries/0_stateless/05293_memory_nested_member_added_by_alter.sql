-- A member of a `Nested` added by `ALTER TABLE ... ADD COLUMN` to a `Memory` table is missing from the blocks inserted
-- before the `ALTER`. For those rows it is read as arrays of default values with the sizes of the other members of the
-- same `Nested` that the table still has, whichever other columns the query reads. An array whose name has no dot is not
-- a member.

DROP TABLE IF EXISTS t_memory_nested_member;
CREATE TABLE t_memory_nested_member (k UInt64, o Nested(p UInt64), n Nested(a UInt64)) ENGINE = Memory;
INSERT INTO t_memory_nested_member VALUES (1, [1, 2, 3], [10, 20]), (2, [], [30]);
ALTER TABLE t_memory_nested_member ADD COLUMN `n.b` Array(UInt64);
ALTER TABLE t_memory_nested_member ADD COLUMN `n.c` Array(Array(UInt64));
ALTER TABLE t_memory_nested_member ADD COLUMN `n.d` Array(Nullable(String));
ALTER TABLE t_memory_nested_member ADD COLUMN `n.t` Array(Tuple(x UInt64, y String));
ALTER TABLE t_memory_nested_member ADD COLUMN `n.j` Array(JSON);
INSERT INTO t_memory_nested_member VALUES (3, [4], [40], [5], [[6, 7]], ['e'], [(8, 'f')], ['{"q" : 9}']), (4, [], [], [], [], [], [], []);
ALTER TABLE t_memory_nested_member ADD COLUMN `m.x` Array(UInt64);

SELECT 'without the other members';
SELECT k, n.b, n.c, n.d, n.t, m.x FROM t_memory_nested_member ORDER BY k;
SELECT 'subcolumns';
SELECT k, n.b.size0, n.c.size0, n.c.size1, n.d.null, n.t.x, n.j.q FROM t_memory_nested_member ORDER BY k;
SELECT k, n.a, n.b.size0, n.c.size1 FROM t_memory_nested_member ORDER BY k;
SELECT 'filters';
SELECT k FROM t_memory_nested_member WHERE has(n.b, 0) ORDER BY k SETTINGS optimize_move_to_prewhere = 0;
SELECT k, n.a FROM t_memory_nested_member PREWHERE has(n.b, 0) ORDER BY k;
SELECT k, n.a, n.b FROM t_memory_nested_member PREWHERE has(n.a, 10) OR has(n.a, 30) ORDER BY k;
SELECT k, arrayMap((x, y) -> x + y, n.a, n.b) FROM t_memory_nested_member PREWHERE has(n.a, 10) OR has(n.a, 30) ORDER BY k;
SELECT k FROM t_memory_nested_member PREWHERE n.b.size0 = 2 ORDER BY k;
SELECT 'ARRAY JOIN';
SELECT k, b FROM t_memory_nested_member ARRAY JOIN n.b AS b ORDER BY k;
SELECT 'row policy';
DROP ROW POLICY IF EXISTS policy_t_memory_nested_member ON t_memory_nested_member;
CREATE ROW POLICY policy_t_memory_nested_member ON t_memory_nested_member FOR SELECT USING empty(n.b) TO CURRENT_USER;
SELECT k FROM t_memory_nested_member ORDER BY k;
DROP ROW POLICY policy_t_memory_nested_member ON t_memory_nested_member;
DROP TABLE t_memory_nested_member;

SELECT 'compress';
DROP TABLE IF EXISTS t_memory_nested_member_compressed;
CREATE TABLE t_memory_nested_member_compressed (k UInt64, n Nested(a UInt64)) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_nested_member_compressed VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_memory_nested_member_compressed ADD COLUMN `n.b` Array(UInt64);
SELECT k, n.b, n.b.size0 FROM t_memory_nested_member_compressed ORDER BY k;
SELECT k, n.a FROM t_memory_nested_member_compressed PREWHERE has(n.b, 0) ORDER BY k;
DROP TABLE t_memory_nested_member_compressed;

SELECT 'dropped members';
DROP TABLE IF EXISTS t_memory_nested_member_dropped;
CREATE TABLE t_memory_nested_member_dropped (k UInt64, n Nested(a UInt64, b UInt64)) ENGINE = Memory;
INSERT INTO t_memory_nested_member_dropped VALUES (1, [10, 20], [1, 2]), (2, [30], [3]);
ALTER TABLE t_memory_nested_member_dropped ADD COLUMN `n.c` Array(UInt64);
ALTER TABLE t_memory_nested_member_dropped DROP COLUMN `n.a`;
SELECT k, n.c, n.c.size0 FROM t_memory_nested_member_dropped ORDER BY k;
ALTER TABLE t_memory_nested_member_dropped DROP COLUMN `n.b`;
SELECT k, n.c, n.c.size0 FROM t_memory_nested_member_dropped ORDER BY k;
DROP TABLE t_memory_nested_member_dropped;

SELECT 'not a member';
DROP TABLE IF EXISTS t_memory_nested_member_plain;
CREATE TABLE t_memory_nested_member_plain (k UInt64, n Array(UInt64)) ENGINE = Memory;
INSERT INTO t_memory_nested_member_plain VALUES (1, [1, 2, 3]);
ALTER TABLE t_memory_nested_member_plain ADD COLUMN `n.a` Array(UInt64);
INSERT INTO t_memory_nested_member_plain VALUES (2, [1, 2, 3], [4]);
ALTER TABLE t_memory_nested_member_plain ADD COLUMN `n.b` Array(UInt64);
SELECT k, n.a, length(n.a) FROM t_memory_nested_member_plain ORDER BY k;
SELECT k, n.b, length(n.b) FROM t_memory_nested_member_plain ORDER BY k;
SELECT k, n, n.a, n.b FROM t_memory_nested_member_plain ORDER BY k;
DROP TABLE t_memory_nested_member_plain;
DROP TABLE IF EXISTS t_memory_nested_member_readded;
CREATE TABLE t_memory_nested_member_readded (k UInt64, n Nested(a UInt64)) ENGINE = Memory;
INSERT INTO t_memory_nested_member_readded VALUES (1, [7, 8]);
ALTER TABLE t_memory_nested_member_readded DROP COLUMN `n.a`;
ALTER TABLE t_memory_nested_member_readded ADD COLUMN n Array(UInt64);
ALTER TABLE t_memory_nested_member_readded ADD COLUMN `n.a` Array(UInt64);
ALTER TABLE t_memory_nested_member_readded ADD COLUMN `n.b` Array(UInt64);
SELECT k, n, ignore(n.b) FROM t_memory_nested_member_readded ORDER BY k;
SELECT k, n, ignore(n.a), ignore(n.b) FROM t_memory_nested_member_readded ORDER BY k;
DROP TABLE t_memory_nested_member_readded;
SELECT 'only the outer sizes';
DROP TABLE IF EXISTS t_memory_nested_member_shadow;
CREATE TABLE t_memory_nested_member_shadow (k UInt64, n Nested(a Tuple(x Array(UInt8)))) ENGINE = Memory;
INSERT INTO t_memory_nested_member_shadow VALUES (1, [([10, 20]), ([30])]);
ALTER TABLE t_memory_nested_member_shadow ADD COLUMN `n.a.x` Array(Array(UInt8));
SELECT k, n.a.x FROM t_memory_nested_member_shadow ORDER BY k;
SELECT k, n.a, n.a.x FROM t_memory_nested_member_shadow ORDER BY k;
DROP TABLE t_memory_nested_member_shadow;
