SET send_logs_level = 'fatal';
SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

DROP TABLE IF EXISTS test_00597;
DROP TABLE IF EXISTS test_view_00597;

CREATE TABLE test_00597(date Date, id Int8, name String, value Int64) ENGINE = MergeTree(date, (id, date), 8192);
CREATE VIEW test_view_00597 AS SELECT * FROM test_00597;

SELECT * FROM (SELECT floor(floor(1, floor(NULL), id = 257), floor(floor(floor(floor(NULL), '10485.76', '9223372036854775807', NULL), floor(10, floor(65535, NULL), 100.0000991821289), NULL)), '2.56'), b.* FROM (SELECT floor(floor(floor(floor(NULL), 1000.0001220703125))), * FROM test_00597) AS b) WHERE id = 257;

INSERT INTO test_00597 VALUES('2000-01-01', 1, 'test string 1', 1);
INSERT INTO test_00597 VALUES('2000-01-01', 2, 'test string 2', 2);

SET enable_optimize_predicate_expression = 1;

SELECT '-------No need for predicate optimization, but still works-------';
SELECT 1;
SELECT 1 AS id WHERE id = 1;
SELECT arrayJoin([1,2,3]) AS id WHERE id = 1;
SELECT * FROM test_00597 WHERE id = 1;

SELECT '-------Forbid push down-------';

-- ARRAY JOIN
EXPLAIN SYNTAX SELECT count() FROM (SELECT [number] a, [number * 2] b FROM system.numbers LIMIT 1) AS t ARRAY JOIN a, b WHERE NOT ignore(a + b);
SELECT count() FROM (SELECT [number] a, [number * 2] b FROM system.numbers LIMIT 1) AS t ARRAY JOIN a, b WHERE NOT ignore(a + b);

-- LEFT JOIN
EXPLAIN SYNTAX SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

-- RIGHT JOIN
EXPLAIN SYNTAX SELECT a, b FROM (SELECT 1 AS a, 1 as b) ANY RIGHT JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a, 1 as b) ANY RIGHT JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;

-- FULL JOIN
EXPLAIN SYNTAX SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

EXPLAIN SYNTAX SELECT a, b FROM (SELECT 1 AS a, 1 AS b) ANY FULL JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

SELECT '-------Need push down-------';

EXPLAIN SYNTAX SELECT toString(value) AS value FROM (SELECT 1 AS value) WHERE value = '1';
SELECT toString(value) AS value FROM (SELECT 1 AS value) WHERE value = '1';

EXPLAIN SYNTAX SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) WHERE id = 1;
SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) WHERE id = 1;

EXPLAIN SYNTAX SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;
SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;

EXPLAIN SYNTAX SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;
SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;

EXPLAIN SYNTAX SELECT * FROM (SELECT 1 AS id, (SELECT 1) as subquery) WHERE subquery = 1;
SELECT * FROM (SELECT 1 AS id, (SELECT 1) as subquery) WHERE subquery = 1;

-- Optimize predicate expressions using tables
EXPLAIN SYNTAX SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test_00597) WHERE a = 3;
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test_00597) WHERE a = 3;

EXPLAIN SYNTAX SELECT date, id, name, value FROM (SELECT date, name, value, min(id) AS id FROM test_00597 GROUP BY date, name, value) WHERE id = 1;
SELECT date, id, name, value FROM (SELECT date, name, value, min(id) AS id FROM test_00597 GROUP BY date, name, value) WHERE id = 1;

EXPLAIN SYNTAX SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test_00597 AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3;
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test_00597 AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3;

-- Optimize predicate expression with asterisk
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597) WHERE id = 1;
SELECT * FROM (SELECT * FROM test_00597) WHERE id = 1;

-- Optimize predicate expression with asterisk and nested subquery
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597)) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597)) WHERE id = 1;

-- Optimize predicate expression with qualified asterisk
EXPLAIN SYNTAX SELECT * FROM (SELECT b.* FROM (SELECT * FROM test_00597) AS b) WHERE id = 1;
SELECT * FROM (SELECT b.* FROM (SELECT * FROM test_00597) AS b) WHERE id = 1;

-- Optimize predicate expression without asterisk
EXPLAIN SYNTAX SELECT * FROM (SELECT date, id, name, value FROM test_00597) WHERE id = 1;
SELECT * FROM (SELECT date, id, name, value FROM test_00597) WHERE id = 1;

-- Optimize predicate expression without asterisk and contains nested subquery
EXPLAIN SYNTAX SELECT * FROM (SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test_00597)) WHERE id = 1;
SELECT * FROM (SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test_00597)) WHERE id = 1;

-- Optimize predicate expression with qualified
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597) AS b WHERE b.id = 1;
SELECT * FROM (SELECT * FROM test_00597) AS b WHERE b.id = 1;

-- Optimize predicate expression with qualified and nested subquery
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) AS a) AS b WHERE b.id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) AS a) AS b WHERE b.id = 1;

-- Optimize predicate expression with aggregate function
EXPLAIN SYNTAX SELECT * FROM (SELECT id, date, min(value) AS value FROM test_00597 GROUP BY id, date) WHERE id = 1;
SELECT * FROM (SELECT id, date, min(value) AS value FROM test_00597 GROUP BY id, date) WHERE id = 1;

-- Optimize predicate expression with union all query
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597 UNION ALL SELECT * FROM test_00597) WHERE id = 1;
SELECT * FROM (SELECT * FROM test_00597 UNION ALL SELECT * FROM test_00597) WHERE id = 1;

-- Optimize predicate expression with join query
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) USING id WHERE id = 1;
SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) USING id WHERE id = 1;

EXPLAIN SYNTAX SELECT * FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test_00597 USING id WHERE value = 1;
SELECT * FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test_00597 USING id WHERE value = 1;

-- FIXME: no support for aliased tables for now.
EXPLAIN SYNTAX SELECT b.value FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test_00597 AS b USING id WHERE value = 1;
SELECT b.value FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test_00597 AS b USING id WHERE value = 1;

-- Optimize predicate expression with join and nested subquery
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) USING id) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) USING id) WHERE id = 1;

-- Optimize predicate expression with join query and qualified
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) AS b USING id WHERE b.id = 1;
SELECT * FROM (SELECT * FROM test_00597) ANY LEFT JOIN (SELECT * FROM test_00597) AS b USING id WHERE b.id = 1;

-- Compatibility test
EXPLAIN SYNTAX SELECT * FROM (SELECT toInt8(1) AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test_00597) AS b USING date, id WHERE b.date = toDate('2000-01-01');
SELECT * FROM (SELECT toInt8(1) AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test_00597) AS b USING date, id WHERE b.date = toDate('2000-01-01');

EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) AS a ANY LEFT JOIN (SELECT * FROM test_00597) AS b  ON  a.id = b.id) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test_00597) AS a ANY LEFT JOIN (SELECT * FROM test_00597) AS b  ON  a.id = b.id) WHERE id = 1;

-- Explain with join subquery
EXPLAIN SYNTAX SELECT * FROM (SELECT * FROM test_00597) ANY INNER JOIN (SELECT * FROM (SELECT * FROM test_00597)) as r USING id WHERE r.id = 1;
SELECT * FROM (SELECT * FROM test_00597) ANY INNER JOIN (SELECT * FROM (SELECT * FROM test_00597)) as r USING id WHERE r.id = 1;

-- issue 20497
EXPLAIN SYNTAX SELECT value + t1.value AS expr FROM (SELECT t0.value, t1.value FROM test_00597 AS t0 FULL JOIN test_00597 AS t1 USING date) WHERE expr < 3;
SELECT value + t1.value AS expr FROM (SELECT t0.value, t1.value FROM test_00597 AS t0 FULL JOIN test_00597 AS t1 USING date) WHERE expr < 3;

DROP TABLE IF EXISTS test_00597;
DROP TABLE IF EXISTS test_view_00597;
