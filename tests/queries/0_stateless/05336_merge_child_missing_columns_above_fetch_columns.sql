-- A `Merge` child that lacks a column computes the query with the column's default. With a `Distributed`
-- child the `Merge` is read above `FetchColumns`, and every value must equal the one of the same query
-- over the same children without `Distributed`.

CREATE TABLE t_a (id UInt64, a UInt8, b UInt8, i Int64, s String, n Nullable(UInt8), arr Array(UInt8), lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_b (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_a VALUES (1, 5, 5, 5, 'x', 7, [1], 'y');
INSERT INTO t_b VALUES (10), (11);
CREATE TABLE t_dist_a AS t_a ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_a);
CREATE TABLE t_dist_b AS t_b ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_b);

-- `(a|dist_b)`: the `Distributed` child lacks the columns; `(dist_a|b)`: the `MergeTree` child does.
SELECT 'min', min(a), min(b), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'min', min(a), min(b), count() FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'avg', avg(a), avg(b) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'avg', avg(a), avg(b) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'uniq', uniqExact(a), uniqExact(b) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'uniq', uniqExact(a), uniqExact(b) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'distinct', count(DISTINCT a), count(DISTINCT b) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'distinct', count(DISTINCT a), count(DISTINCT b) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'fuse', sum(id + a), avg(id + b) FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS optimize_syntax_fuse_functions = 1;
SELECT 'fuse', sum(id + a), avg(id + b) FROM merge(currentDatabase(), '^t_(dist_a|b)$') SETTINGS optimize_syntax_fuse_functions = 1;
SELECT 'expr', sum(a * b), count(a), min(a + 1) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'expr', sum(a * b), count(a), min(a + 1) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'fold', min(a * b), min(a * a), count(a * b), count(b * b) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'fold', min(a * b), min(a * a), count(a * b), count(b * b) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'types', min(i), min(s), max(n), countIf(n IS NULL), groupArraySorted(10)(arr), min(lc) FROM merge(currentDatabase(), '^t_(a|dist_b)$');
SELECT 'types', min(i), min(s), max(n), countIf(n IS NULL), groupArraySorted(10)(arr), min(lc) FROM merge(currentDatabase(), '^t_(dist_a|b)$');
SELECT 'having', id FROM merge(currentDatabase(), '^t_(a|dist_b)$') GROUP BY id HAVING min(a) = 0 AND min(b) = 0 ORDER BY id;
SELECT 'having', id FROM merge(currentDatabase(), '^t_(dist_a|b)$') GROUP BY id HAVING min(a) = 0 AND min(b) = 0 ORDER BY id;
SELECT 'where', countIf(a = 5), countIf(a = 0) FROM merge(currentDatabase(), '^t_(a|dist_b)$') WHERE id > 0;
SELECT 'where', countIf(a = 5), countIf(a = 0) FROM merge(currentDatabase(), '^t_(dist_a|b)$') WHERE id > 0;
SELECT 'filter', count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') WHERE a = 5;
SELECT 'filter', count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') WHERE a = 5;
SELECT 'filter', count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') WHERE a = 0;
SELECT 'filter', count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') WHERE a = 0;

-- Columns of a subquery over the `Merge`.
SELECT 'rows', arraySort(groupArray((id, a, b))) FROM (SELECT id, a, b FROM merge(currentDatabase(), '^t_(a|dist_b)$') ORDER BY id);
SELECT 'rows', arraySort(groupArray((id, a, b))) FROM (SELECT id, a, b FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id);
SELECT 'rows expr', arraySort(groupArray((id, y))) FROM (SELECT id, a + 1 AS y FROM merge(currentDatabase(), '^t_(a|dist_b)$') ORDER BY id);
SELECT 'rows expr', arraySort(groupArray((id, y))) FROM (SELECT id, a + 1 AS y FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id);
SELECT 'rows null', arraySort(groupArray((id, n.null))) FROM (SELECT id, n.null FROM merge(currentDatabase(), '^t_(a|dist_b)$') ORDER BY id);
SELECT 'rows null', arraySort(groupArray((id, n.null))) FROM (SELECT id, n.null FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id);

-- The default of a subcolumn and of a key that is an expression over a missing column.
SELECT 'null', id, n.null FROM merge(currentDatabase(), '^t_(a|dist_b)$') ORDER BY id;
SELECT 'null', id, n.null FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id;
SELECT 'key', id, a + 1 AS y, count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') GROUP BY id, y ORDER BY id;

-- A `Merge` table with a single `Distributed` child is read at `Complete`.
CREATE TABLE t_m1 (id UInt64, a UInt8, b UInt8) ENGINE = Merge(currentDatabase(), '^t_dist_b$');
SELECT 'complete', min(a), min(b), count() FROM t_m1;

-- A `Buffer` child plans its buffers itself.
CREATE TABLE t_c (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_c VALUES (20);
CREATE TABLE t_dist_c AS t_c ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_c);
CREATE TABLE t_buf_c AS t_c ENGINE = Buffer(currentDatabase(), t_dist_c, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_buf_c VALUES (21);
SELECT 'buffer', min(a), min(b), count() FROM merge(currentDatabase(), '^t_(dist_a|buf_c)$');
SELECT 'buffer fuse', sum(id + a), avg(id + b) FROM merge(currentDatabase(), '^t_(dist_a|buf_c)$') SETTINGS optimize_syntax_fuse_functions = 1;
