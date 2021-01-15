DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt8, b UInt16) ENGINE = TinyLog;
CREATE TABLE t2 (a String, b Nullable(Int64)) ENGINE = TinyLog;

INSERT INTO t1
SELECT number + 2 as a, 100 + number + 2 as b FROM system.numbers LIMIT 1, 15;

INSERT INTO t2
SELECT number as a, 200 + number as b FROM system.numbers LIMIT 1, 10;
INSERT INTO t2 VALUES ('-1', NULL);

SET join_algorithm = 'hash';

SELECT '--- hash ---';

SELECT '- full -';
SELECT * FROM t1 FULL JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT * FROM t1 FULL JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a)); -- { serverError 53 }
SELECT '- left -';
SELECT * FROM t1 LEFT JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- left using -';
SELECT * FROM t1 LEFT JOIN t2 USING(a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- right -';
SELECT * FROM t1 RIGHT JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
-- SELECT * FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner -';
SELECT * FROM t1 INNER JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner using -';
SELECT * FROM t1 INNER JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));

SELECT '- swap tables -';
SELECT '- full -';
SELECT * FROM t2 FULL JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- left -';
SELECT * FROM t2 LEFT JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- right -';
SELECT * FROM t2 RIGHT JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner -';
SELECT * FROM t2 INNER JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));

SELECT '- types -';
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 FULL JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 LEFT JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 RIGHT JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 INNER JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 INNER JOIN t2 USING (a);

SET join_algorithm = 'partial_merge';

SELECT '--- partial_merge ---';

SELECT '- full -';
SELECT * FROM t1 FULL JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT * FROM t1 FULL JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a)); -- { serverError 53 }
SELECT '- left -';
SELECT * FROM t1 LEFT JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- left using -';
SELECT * FROM t1 LEFT JOIN t2 USING(a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- right -';
SELECT * FROM t1 RIGHT JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
-- SELECT * FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner -';
SELECT * FROM t1 INNER JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner using -';
SELECT * FROM t1 INNER JOIN t2 USING (a) ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));

SELECT '- swap tables -';
SELECT '- full -';
SELECT * FROM t2 FULL JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- left -';
SELECT * FROM t2 LEFT JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- right -';
SELECT * FROM t2 RIGHT JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner -';
SELECT * FROM t2 INNER JOIN t1 ON t1.a == t2.a ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));

SELECT '- types -';
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 FULL JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 LEFT JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 RIGHT JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 INNER JOIN t2 ON t1.a == t2.a;
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 INNER JOIN t2 USING (a);

SET join_algorithm = 'auto';

SELECT '--- complex ON condition ---';

SELECT '- full -';
SELECT * FROM t1 FULL JOIN t2 ON t1.a == t2.a AND t1.b + 101 == t2.b + 1
ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- left -';
SELECT * FROM t1 LEFT JOIN t2 ON t1.a == t2.a AND t1.b + 101 == t2.b + 1
ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- right -';
SELECT * FROM t1 RIGHT JOIN t2 ON t1.a == t2.a AND t1.b + 101 == t2.b + 1
ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));
SELECT '- inner -';
SELECT * FROM t1 INNER JOIN t2 ON t1.a == t2.a AND t1.b + 101 == t2.b + 1
ORDER BY (toInt32(if(empty(t2.a), '0', t2.a)), toInt32(t1.a));

SET join_use_nulls = 1;

SELECT '--- join_use_nulls ---';
SELECT any(toTypeName(a)), any(toTypeName(t2.a)) FROM t1 FULL JOIN t2 ON t1.a == t2.a;
SELECT * FROM t1 FULL JOIN t2 ON t1.a == t2.a ORDER BY (toInt32(ifNull(t2.a, '0')), toInt32(t1.a));

SET join_use_nulls = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

SELECT '--- non-convertable values ---';

CREATE TABLE t1 (a Nullable(Int32), b String) ENGINE = Memory;
CREATE TABLE t2 (a UInt8, b String) ENGINE = Memory;

INSERT INTO t1 VALUES (-1, 'a');
INSERT INTO t1 VALUES (1, 'b');
INSERT INTO t1 VALUES (NULL, 'c');
INSERT INTO t1 VALUES (0, 'd');
INSERT INTO t1 VALUES (301, '11');

INSERT INTO t2 VALUES (1, '10');
INSERT INTO t2 VALUES (1, '11');
INSERT INTO t2 VALUES (2, '20');

SELECT '- full -';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- left -';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- right -';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- inner -';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);

SET join_algorithm = 'partial_merge';

SELECT '- full -';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- left -';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- right -';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);
SELECT '- inner -';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON t1.a == t2.a ORDER BY (t1.b, t2.b);

SET join_algorithm = 'hash';
select '-';
SELECT * FROM t1 JOIN t2 ON t1.a + 10 == t2.b ORDER BY a;
select '-';
SELECT * FROM t1 JOIN t2 ON t1.b == t2.a;
SELECT COUNT(*) FROM t1 JOIN t2 ON t1.b == t2.a;
select '-';
SELECT * FROM t1 JOIN t2 ON t1.b == t2.b;

SET join_algorithm = 'partial_merge';
select '-';
SELECT * FROM t1 JOIN t2 ON t1.a + 10 == t2.b ORDER BY a;
select '-';
SELECT * FROM t1 JOIN t2 ON t1.b == t2.a;
SELECT COUNT(*) FROM t1 JOIN t2 ON t1.b == t2.a;
select '-';
SELECT * FROM t1 JOIN t2 ON t1.b == t2.b;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

select '--- multiple keys ---';

CREATE TABLE t1 (a Nullable(String), b String, c String) ENGINE = TinyLog;
CREATE TABLE t2 (a UInt64, b Nullable(UInt8), c String) ENGINE = TinyLog;
INSERT INTO t1 VALUES ('-1', '-1', '-1'), ('0', '0', '0'), ('1', '1', '1'), ('2', '2', '2');
INSERT INTO t2 VALUES (1, 1, '1'), (2, 2, '2'), (10, 10, 10);

SET join_algorithm = 'hash';

SELECT '--- hash ---';

SELECT '-';
SELECT * FROM t1 INNER JOIN t2 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 LEFT JOIN t2 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
-- SELECT * FROM t1 RIGHT JOIN t2 USING (a, b, c);
SELECT * FROM t1 FULL JOIN t2 USING (a, b, c); -- { serverError 53 }

SELECT '-';
SELECT * FROM t2 INNER JOIN t1 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 LEFT JOIN t1 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
-- SELECT * FROM t2 RIGHT JOIN t1 USING (a, b, c);
SELECT * FROM t2 FULL JOIN t1 USING (a, b, c); -- { serverError 53 }

SELECT '-';
SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 FULL JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);

SELECT '-';
SELECT * FROM t2 INNER JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 LEFT JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 RIGHT JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 FULL JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);

SET join_algorithm = 'partial_merge';

SELECT '--- partial_merge ---';

SELECT '-';
SELECT * FROM t1 INNER JOIN t2 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 LEFT JOIN t2 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
-- SELECT * FROM t1 RIGHT JOIN t2 USING (a, b, c);
SELECT * FROM t1 FULL JOIN t2 USING (a, b, c); -- { serverError 53 }

SELECT '-';
SELECT * FROM t2 INNER JOIN t1 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 LEFT JOIN t1 USING (a, b, c) ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
-- SELECT * FROM t2 RIGHT JOIN t1 USING (a, b, c);
SELECT * FROM t2 FULL JOIN t1 USING (a, b, c); -- { serverError 53 }

SELECT '-';
SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t1 FULL JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);

SELECT '-';
SELECT * FROM t2 INNER JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 LEFT JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 RIGHT JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);
SELECT '-';
SELECT * FROM t2 FULL JOIN t1 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY (t1.a, t1.b, t1.c, t2.a, t2.b, t2.c);

SET join_algorithm = 'auto';

SELECT * FROM ( SELECT [1] as a ) AS t1 JOIN ( SELECT ['1'] as a ) AS t2 ON t1.a == t2.a; -- { serverError 53 }
SELECT * FROM ( SELECT [1] as a ) AS t1 JOIN ( SELECT 1 as a ) AS t2 ON t1.a == t2.a; -- { serverError 53 }
SELECT * FROM ( SELECT 1 as a ) AS t1 JOIN ( SELECT [1] as a ) AS t2 ON t1.a == t2.a; -- { serverError 53 }
SELECT * FROM ( SELECT toLowCardinality('1') as a ) AS t1 JOIN ( SELECT 1 as a ) AS t2 ON t1.a == t2.a; -- { serverError 53 }
SELECT * FROM ( SELECT 1 as a ) AS t1 JOIN ( SELECT toLowCardinality('1') as a ) AS t2 ON t1.a == t2.a; -- { serverError 53 }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;



