DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt16, b UInt16) ENGINE = TinyLog;
CREATE TABLE t2 (a Int16, b Nullable(Int64)) ENGINE = TinyLog;

INSERT INTO t1 SELECT number as a, 100 + number as b FROM system.numbers LIMIT 1, 10;
INSERT INTO t2 SELECT number - 5 as a, 200 + number - 5 as b FROM system.numbers LIMIT 1, 10;

SELECT '=== hash ===';
SET join_algorithm = 'hash';

SELECT '= full =';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '= left =';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '= right =';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '= inner =';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);

SELECT '= full =';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '= join on =';
SELECT '= full =';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);

SELECT '= full =';
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);

-- Int64 and UInt64 has no supertype
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }

SELECT '= agg =';
SELECT sum(a) == 7 FROM t1 FULL JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;
SELECT sum(a) == 7 FROM t1 INNER JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;

SELECT sum(b) = 103 FROM t1 LEFT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;
SELECT sum(t2.b) = 203 FROM t1 RIGHT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;

SELECT sum(a) == 2 + 3 + 4 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE t1.b < 105 AND t2.b > 201;
SELECT sum(a) == 55 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE 1;

SELECT a > 0, sum(a), sum(b) FROM t1 FULL JOIN t2 USING (a) GROUP BY (a > 0) ORDER BY a > 0;
SELECT a > 0, sum(a), sum(t2.a), sum(b), sum(t2.b) FROM t1 FULL JOIN t2 ON (t1.a == t2.a) GROUP BY (a > 0) ORDER BY a > 0;

SELECT '= types =';
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT toTypeName(any(a)) == 'Int32' AND toTypeName(any(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT min(toTypeName(a) == 'Int32' AND toTypeName(t2.a) == 'Int32') FROM t1 FULL JOIN t2 USING (a);

SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 LEFT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 INNER JOIN t2 ON (t1.a == t2.a);
SELECT toTypeName(any(a)) == 'UInt16' AND toTypeName(any(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);

SELECT '=== partial_merge ===';

SET join_algorithm = 'partial_merge';

SELECT '= full =';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '= left =';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '= right =';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '= inner =';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);

SELECT '= full =';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '= join on =';
SELECT '= full =';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);

SELECT '= full =';
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);

-- Int64 and UInt64 has no supertype
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }

SELECT '= agg =';
SELECT sum(a) == 7 FROM t1 FULL JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;
SELECT sum(a) == 7 FROM t1 INNER JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;

SELECT sum(b) = 103 FROM t1 LEFT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;
SELECT sum(t2.b) = 203 FROM t1 RIGHT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;

SELECT sum(a) == 2 + 3 + 4 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE t1.b < 105 AND t2.b > 201;
SELECT sum(a) == 55 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE 1;

SELECT a > 0, sum(a), sum(b) FROM t1 FULL JOIN t2 USING (a) GROUP BY (a > 0) ORDER BY a > 0;
SELECT a > 0, sum(a), sum(t2.a), sum(b), sum(t2.b) FROM t1 FULL JOIN t2 ON (t1.a == t2.a) GROUP BY (a > 0) ORDER BY a > 0;

SELECT '= types =';
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT toTypeName(any(a)) == 'Int32' AND toTypeName(any(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT min(toTypeName(a) == 'Int32' AND toTypeName(t2.a) == 'Int32') FROM t1 FULL JOIN t2 USING (a);

SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 LEFT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 INNER JOIN t2 ON (t1.a == t2.a);
SELECT toTypeName(any(a)) == 'UInt16' AND toTypeName(any(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);

SELECT '=== switch ===';

SET join_algorithm = 'auto';
SET max_bytes_in_join = 100;

SELECT '= full =';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '= left =';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '= right =';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '= inner =';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);

SELECT '= full =';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '= join on =';
SELECT '= full =';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);

SELECT '= full =';
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);

-- Int64 and UInt64 has no supertype
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }

SELECT '= agg =';
SELECT sum(a) == 7 FROM t1 FULL JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;
SELECT sum(a) == 7 FROM t1 INNER JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;

SELECT sum(b) = 103 FROM t1 LEFT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;
SELECT sum(t2.b) = 203 FROM t1 RIGHT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;

SELECT sum(a) == 2 + 3 + 4 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE t1.b < 105 AND t2.b > 201;
SELECT sum(a) == 55 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE 1;

SELECT a > 0, sum(a), sum(b) FROM t1 FULL JOIN t2 USING (a) GROUP BY (a > 0) ORDER BY a > 0;
SELECT a > 0, sum(a), sum(t2.a), sum(b), sum(t2.b) FROM t1 FULL JOIN t2 ON (t1.a == t2.a) GROUP BY (a > 0) ORDER BY a > 0;

SELECT '= types =';
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT toTypeName(any(a)) == 'Int32' AND toTypeName(any(t2.a)) == 'Int32' FROM t1 FULL JOIN t2 USING (a);
SELECT min(toTypeName(a) == 'Int32' AND toTypeName(t2.a) == 'Int32') FROM t1 FULL JOIN t2 USING (a);

SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 LEFT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 INNER JOIN t2 ON (t1.a == t2.a);
SELECT toTypeName(any(a)) == 'UInt16' AND toTypeName(any(t2.a)) == 'Int16' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);

SET max_bytes_in_join = 0;

SELECT '=== join use nulls ===';

SET join_use_nulls = 1;


SELECT '= full =';
SELECT a, b, t2.b FROM t1 FULL JOIN t2 USING (a) ORDER BY (a);
SELECT '= left =';
SELECT a, b, t2.b FROM t1 LEFT JOIN t2 USING (a) ORDER BY (a);
SELECT '= right =';
SELECT a, b, t2.b FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (a);
SELECT '= inner =';
SELECT a, b, t2.b FROM t1 INNER JOIN t2 USING (a) ORDER BY (a);

SELECT '= full =';
SELECT a, t1.a, t2.a FROM t1 FULL JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, t1.a, t2.a FROM t1 LEFT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, t1.a, t2.a FROM t1 RIGHT JOIN t2 USING (a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, t1.a, t2.a FROM t1 INNER JOIN t2 USING (a) ORDER BY (t1.a, t2.a);

SELECT '= join on =';
SELECT '= full =';
SELECT a, b, t2.a, t2.b FROM t1 FULL JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT a, b, t2.a, t2.b FROM t1 LEFT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT a, b, t2.a, t2.b FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT a, b, t2.a, t2.b FROM t1 INNER JOIN t2 ON (t1.a == t2.a) ORDER BY (t1.a, t2.a);

SELECT '= full =';
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= left =';
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= right =';
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);
SELECT '= inner =';
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) ORDER BY (t1.a, t2.a);

-- Int64 and UInt64 has no supertype
SELECT * FROM t1 FULL JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 LEFT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 RIGHT JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }
SELECT * FROM t1 INNER JOIN t2 ON (t1.a + t1.b + 100 = t2.a + t2.b) ORDER BY (t1.a, t2.a); -- { serverError 53 }

SELECT '= agg =';
SELECT sum(a) == 7 FROM t1 FULL JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;
SELECT sum(a) == 7 FROM t1 INNER JOIN t2 USING (a) WHERE b > 102 AND t2.b <= 204;

SELECT sum(b) = 103 FROM t1 LEFT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;
SELECT sum(t2.b) = 203 FROM t1 RIGHT JOIN t2 USING (a) WHERE b > 102 AND t2.b < 204;

SELECT sum(a) == 2 + 3 + 4 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE t1.b < 105 AND t2.b > 201;
SELECT sum(a) == 55 FROM t1 FULL JOIN t2 ON (t1.a + t1.b = t2.a + t2.b - 100) WHERE 1;

SELECT a > 0, sum(a), sum(b) FROM t1 FULL JOIN t2 USING (a) GROUP BY (a > 0) ORDER BY a > 0;
SELECT a > 0, sum(a), sum(t2.a), sum(b), sum(t2.b) FROM t1 FULL JOIN t2 ON (t1.a == t2.a) GROUP BY (a > 0) ORDER BY a > 0;

SELECT '= types =';
SELECT any(toTypeName(a)) == 'Nullable(Int32)' AND any(toTypeName(t2.a)) == 'Nullable(Int32)' FROM t1 FULL JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Nullable(Int32)' FROM t1 LEFT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Nullable(Int32)' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 RIGHT JOIN t2 USING (a);
SELECT any(toTypeName(a)) == 'Int32' AND any(toTypeName(t2.a)) == 'Int32' FROM t1 INNER JOIN t2 USING (a);

SELECT toTypeName(any(a)) == 'Nullable(Int32)' AND toTypeName(any(t2.a)) == 'Nullable(Int32)' FROM t1 FULL JOIN t2 USING (a);
SELECT min(toTypeName(a) == 'Nullable(Int32)' AND toTypeName(t2.a) == 'Nullable(Int32)') FROM t1 FULL JOIN t2 USING (a);

SELECT any(toTypeName(a)) == 'Nullable(UInt16)' AND any(toTypeName(t2.a)) == 'Nullable(Int16)' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Nullable(Int16)' FROM t1 LEFT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'Nullable(UInt16)' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 RIGHT JOIN t2 ON (t1.a == t2.a);
SELECT any(toTypeName(a)) == 'UInt16' AND any(toTypeName(t2.a)) == 'Int16' FROM t1 INNER JOIN t2 ON (t1.a == t2.a);
SELECT toTypeName(any(a)) == 'Nullable(UInt16)' AND toTypeName(any(t2.a)) == 'Nullable(Int16)' FROM t1 FULL JOIN t2 ON (t1.a == t2.a);

SET join_use_nulls = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
