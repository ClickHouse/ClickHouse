DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b Float64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (x UInt32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t1 VALUES (1, 3.14), (2, 2.72);
INSERT INTO t2 VALUES (1, 'hello'), (2, 'world');

SET enable_analyzer = 1;

-- `b` also names the scalar column `t1.b`, but `b.*` must expand the table aliased `b` (t2).
SELECT a, b.* FROM t1 JOIN t2 AS b ON t1.a = b.x ORDER BY a;

-- The setting does not change this: the qualifier is a single-part identifier, not a subcolumn path.
SELECT a, b.* FROM t1 JOIN t2 AS b ON t1.a = b.x ORDER BY a SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

-- Same collision when the alias `b` is a subquery.
SELECT a, b.* FROM t1 JOIN (SELECT x, y FROM t2) AS b ON t1.a = b.x ORDER BY a;

-- Same collision when the alias `b` is a CTE.
WITH cte AS (SELECT x, y FROM t2) SELECT a, b.* FROM t1 JOIN cte AS b ON t1.a = b.x ORDER BY a;

-- No collision: still works.
SELECT a, b.* FROM (SELECT a FROM t1) AS l JOIN t2 AS b ON l.a = b.x ORDER BY a;

-- Tuple-column expansion via a qualified matcher is unaffected.
SELECT b.* FROM (SELECT (1, 'x') AS b);

--- The USING join path is also unaffected
SELECT b.* FROM t1 JOIN (SELECT x AS a, y FROM t2) AS b USING (a) ORDER BY a;
SELECT arrayMap(b -> sipHash64(b.*), [1]) FROM t1 JOIN (SELECT x AS a, y FROM t2) AS b USING (a) ORDER BY a;
SELECT arrayMap(b -> sipHash64(b.*), [1]) FROM t1 JOIN (SELECT x, y FROM t2) AS b ON t1.a = b.x ORDER BY b.x;
WITH 0 AS b SELECT b.* FROM t1 JOIN (SELECT x AS a, y FROM t2) AS b USING (a) ORDER BY a;

-- Guard: when the qualifier resolves to a compound (Tuple) column, the matcher keeps expanding that
-- column's subcolumns even when the same name is also a table. The fix only changes the non-compound
-- case, so this compound path must stay as-is (the compound column wins over the table).
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS tb;
CREATE TABLE b (b Tuple(p UInt32, q String), z UInt32) ENGINE = MergeTree ORDER BY z;
INSERT INTO b VALUES ((10, 'foo'), 1), ((20, 'bar'), 2);

-- Table `b` has a Tuple column also named `b`: `b.*` expands the Tuple subcolumns (p, q).
SELECT b.* FROM b ORDER BY 1;

-- Another table has a Tuple column `b` and is joined with the real table `b`: `b.*` still expands the
-- Tuple column (compound wins over the same-named table).
CREATE TABLE tb (a UInt32, b Tuple(p UInt32, q String)) ENGINE = MergeTree ORDER BY a;
INSERT INTO tb VALUES (1, (10, 'foo')), (2, (20, 'bar'));
SELECT a, b.* FROM tb JOIN b ON tb.a = b.z ORDER BY a;

DROP TABLE b;
DROP TABLE tb;

-- If the qualifier is a non-compound column and there is no table with that name,
-- the precise "non-compound expression" diagnostic is preserved.
SELECT b.* FROM t1; -- { serverError UNSUPPORTED_METHOD }

DROP TABLE t1;
DROP TABLE t2;
