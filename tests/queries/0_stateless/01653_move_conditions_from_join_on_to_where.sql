DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE table2 (a UInt32, b UInt32) ENGINE = Memory;

INSERT INTO table1 SELECT number, number FROM numbers(10);
INSERT INTO table2 SELECT number * 2, number * 20 FROM numbers(6);

SELECT '---------Q1----------';
SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.b = toUInt32(20));
EXPLAIN SYNTAX SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.b = toUInt32(20));

SELECT '---------Q2----------';
SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.a < table2.b) AND (table2.b = toUInt32(20));
EXPLAIN SYNTAX SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.a < table2.b) AND (table2.b = toUInt32(20));

SELECT '---------Q3----------';
SELECT * FROM table1 JOIN table2 ON (table1.a = toUInt32(table2.a + 5)) AND (table2.a < table1.b) AND (table2.b > toUInt32(20)); -- { serverError 48 }

SELECT '---------Q4----------';
SELECT table1.a, table2.b FROM table1 INNER JOIN table2 ON (table1.a = toUInt32(10 - table2.a)) AND (table1.b = 6) AND (table2.b > 20);
EXPLAIN SYNTAX SELECT table1.a, table2.b FROM table1 INNER JOIN table2 ON (table1.a = toUInt32(10 - table2.a)) AND (table1.b = 6) AND (table2.b > 20);

SELECT '---------Q5----------';
SELECT table1.a, table2.b FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table1.b = 6) AND (table2.b > 20) AND (10 < 6);
EXPLAIN SYNTAX SELECT table1.a, table2.b FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table1.b = 6) AND (table2.b > 20) AND (10 < 6);

SELECT '---------Q6----------';
SELECT table1.a, table2.b FROM table1 JOIN table2 ON (table1.b = 6) AND (table2.b > 20); -- { serverError 403 } 

SELECT '---------Q7----------';
SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.b < toUInt32(40)) where table1.b < 1;
EXPLAIN SYNTAX SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.b < toUInt32(40)) where table1.b < 1;
SELECT * FROM table1 JOIN table2 ON (table1.a = table2.a) AND (table2.b < toUInt32(40)) where table1.b > 10;

SELECT '---------Q8----------';
SELECT * FROM table1 INNER JOIN table2 ON (table1.a = table2.a) AND (table2.b < toUInt32(table1, 10)); -- { serverError 47 }

SELECT '---------Q9---will not be optimized----------';
EXPLAIN SYNTAX SELECT * FROM table1 LEFT JOIN table2 ON (table1.a = table2.a) AND (table1.b = toUInt32(10));
EXPLAIN SYNTAX SELECT * FROM table1 RIGHT JOIN table2 ON (table1.a = table2.a) AND (table1.b = toUInt32(10));
EXPLAIN SYNTAX SELECT * FROM table1 FULL JOIN table2 ON (table1.a = table2.a) AND (table1.b = toUInt32(10));
EXPLAIN SYNTAX SELECT * FROM table1 FULL JOIN table2 ON (table1.a = table2.a) AND (table2.b = toUInt32(10)) WHERE table1.a < toUInt32(20);
EXPLAIN SYNTAX SELECT * FROM table1 , table2;

DROP TABLE table1;
DROP TABLE table2;
