DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt16, b UInt16) ENGINE = TinyLog;
CREATE TABLE t2 (a Int16, b Nullable(Int64)) ENGINE = TinyLog;

INSERT INTO t1 SELECT number as a, 100 + number as b FROM system.numbers LIMIT 1, 10;
INSERT INTO t2 SELECT number - 5 as a, 200 + number - 5 as b FROM system.numbers LIMIT 1, 10;

SELECT '--- hash ---';
SET join_algorithm = 'hash';

SELECT '- full -';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '- left -';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '- right -';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '- inner -';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);

SELECT '- full -';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- left -';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- right -';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- inner -';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '- types -';
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT * FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON(t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }

SELECT '--- partial_merge ---';

SET join_algorithm = 'partial_merge';

SELECT '- full -';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '- left -';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '- right -';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '- inner -';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);


SELECT '- full -';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- left -';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- right -';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '- inner -';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '- types -';
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT * FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON(t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (a); -- { serverError 53 }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

DROP TABLE IF EXISTS t_ab1;
DROP TABLE IF EXISTS t_ab2;

CREATE TABLE t_ab1 (id Nullable(Int32), a UInt16, b UInt8) ENGINE = TinyLog;
CREATE TABLE t_ab2 (id Nullable(Int32), a Int16, b Nullable(Int64)) ENGINE = TinyLog;
INSERT INTO t_ab1 VALUES (0, 1, 1), (1, 2, 2);
INSERT INTO t_ab2 VALUES (2, -1, 1), (3, 1, NULL), (4, 1, 257), (5, 1, -1), (6, 1, 1);

SELECT '--- hash ---';

SELECT '- full -';
SELECT a, b FROM t_ab1 FULL JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- left -';
SELECT a, b FROM t_ab1 LEFT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- right -';
SELECT a, b FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- inner -';
SELECT a, b FROM t_ab1 INNER JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '- types -';

SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

SELECT '--- partial_merge ---';

SET join_algorithm = 'partial_merge';

SELECT '- full -';
SELECT a, b FROM t_ab1 FULL JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- left -';
SELECT a, b FROM t_ab1 LEFT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- right -';
SELECT a, b FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '- inner -';
SELECT a, b FROM t_ab1 INNER JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '- types -';

SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

DROP TABLE IF EXISTS t_ab1;
DROP TABLE IF EXISTS t_ab2;
