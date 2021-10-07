DROP TABLE IF EXISTS t_ab1;
DROP TABLE IF EXISTS t_ab2;

CREATE TABLE t_ab1 (id Nullable(Int32), a UInt16, b UInt8) ENGINE = TinyLog;
CREATE TABLE t_ab2 (id Nullable(Int32), a Int16, b Nullable(Int64)) ENGINE = TinyLog;
INSERT INTO t_ab1 VALUES (0, 1, 1), (1, 2, 2);
INSERT INTO t_ab2 VALUES (2, -1, 1), (3, 1, NULL), (4, 1, 257), (5, 1, -1), (6, 1, 1);

SELECT '=== hash ===';

SET join_algorithm = 'hash';

SELECT '= full =';
SELECT a, b FROM t_ab1 FULL JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= left =';
SELECT a, b FROM t_ab1 LEFT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= right =';
SELECT a, b FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= inner =';
SELECT a, b FROM t_ab1 INNER JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '= full =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= left =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 LEFT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= right =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 RIGHT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= inner =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 INNER JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '= agg =';
SELECT sum(a), sum(b) FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 LEFT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 RIGHT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 INNER JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);

SELECT '= types =';

SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

SELECT * FROM ( SELECT a, b as "CAST(a, Int32)" FROM t_ab1 ) t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a); -- { serverError 44 }
SELECT * FROM ( SELECT a, b as "CAST(a, Int32)" FROM t_ab1 ) t_ab1 FULL JOIN t_ab2 USING (a) FORMAT Null;

SELECT '=== partial_merge ===';

SET join_algorithm = 'partial_merge';

SELECT '= full =';
SELECT a, b FROM t_ab1 FULL JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= left =';
SELECT a, b FROM t_ab1 LEFT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= right =';
SELECT a, b FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= inner =';
SELECT a, b FROM t_ab1 INNER JOIN t_ab2 USING (a, b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '= full =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= left =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 LEFT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= right =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 RIGHT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);
SELECT '= inner =';
SELECT a, b, t_ab2.a, t_ab2.b FROM t_ab1 INNER JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b) ORDER BY ifNull(t_ab1.id, t_ab2.id);

SELECT '= agg =';
SELECT sum(a), sum(b) FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT sum(a), sum(b) FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 LEFT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 RIGHT JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);
SELECT sum(a) + sum(t_ab2.a) - 1, sum(b) + sum(t_ab2.b) - 1 FROM t_ab1 INNER JOIN t_ab2 ON (t_ab1.a == t_ab2.a AND t_ab1.b == t_ab2.b);

SELECT '= types =';

SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 FULL JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 LEFT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 RIGHT JOIN t_ab2 USING (a, b);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(b)) == 'Nullable(Int64)' FROM t_ab1 INNER JOIN t_ab2 USING (a, b);

SELECT * FROM ( SELECT a, b as "CAST(a, Int32)" FROM t_ab1 ) t_ab1 FULL JOIN t_ab2 ON (t_ab1.a == t_ab2.a); -- { serverError 44 }
SELECT * FROM ( SELECT a, b as "CAST(a, Int32)" FROM t_ab1 ) t_ab1 FULL JOIN t_ab2 USING (a) FORMAT Null;

DROP TABLE IF EXISTS t_ab1;
DROP TABLE IF EXISTS t_ab2;
