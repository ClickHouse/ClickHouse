SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;

CREATE TABLE test.test(date Date, id Int8, name String, value Int64) ENGINE = MergeTree(date, (id, date), 8192);
CREATE VIEW test.test_view AS SELECT * FROM test.test;

INSERT INTO test.test VALUES('2000-01-01', 1, 'test string 1', 1);
INSERT INTO test.test VALUES('2000-01-01', 2, 'test string 2', 2);

SET enable_optimize_predicate_expression = 1;
SET enable_debug_queries = 1;

SELECT '-------No need for predicate optimization, but still works-------';
SELECT 1;
SELECT 1 AS id WHERE id = 1;
SELECT arrayJoin([1,2,3]) AS id WHERE id = 1;
SELECT * FROM test.test WHERE id = 1;

SELECT '-------Forbid push down-------';

-- ARRAY JOIN
ANALYZE SELECT count() FROM (SELECT [number] a, [number * 2] b FROM system.numbers LIMIT 1) AS t ARRAY JOIN a, b WHERE NOT ignore(a + b);
SELECT count() FROM (SELECT [number] a, [number * 2] b FROM system.numbers LIMIT 1) AS t ARRAY JOIN a, b WHERE NOT ignore(a + b);

-- LEFT JOIN
ANALYZE SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

-- RIGHT JOIN
ANALYZE SELECT a, b FROM (SELECT 1 AS a, 1 as b) ANY RIGHT JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a, 1 as b) ANY RIGHT JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;

-- FULL JOIN
ANALYZE SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

ANALYZE SELECT a, b FROM (SELECT 1 AS a, 1 AS b) ANY FULL JOIN (SELECT 1 AS a) USING (a) WHERE b = 0;
SELECT a, b FROM (SELECT 1 AS a) ANY FULL JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0;

SELECT '-------Need push down-------';

ANALYZE SELECT toString(value) AS value FROM (SELECT 1 AS value) WHERE value = '1';
SELECT toString(value) AS value FROM (SELECT 1 AS value) WHERE value = '1';

ANALYZE SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) WHERE id = 1;
SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) WHERE id = 1;

ANALYZE SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;
SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;

ANALYZE SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;
SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;

ANALYZE SELECT * FROM (SELECT 1 AS id, (SELECT 1) as subquery) WHERE subquery = 1;
SELECT * FROM (SELECT 1 AS id, (SELECT 1) as subquery) WHERE subquery = 1;

-- Optimize predicate expressions using tables
ANALYZE SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test) WHERE a = 3;
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test) WHERE a = 3;

ANALYZE SELECT date, id, name, value FROM (SELECT date, name, value, min(id) AS id FROM test.test GROUP BY date, name, value) WHERE id = 1;
SELECT date, id, name, value FROM (SELECT date, name, value, min(id) AS id FROM test.test GROUP BY date, name, value) WHERE id = 1;

ANALYZE SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3;
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3;

-- Optimize predicate expression with asterisk
ANALYZE SELECT * FROM (SELECT * FROM test.test) WHERE id = 1;
SELECT * FROM (SELECT * FROM test.test) WHERE id = 1;

-- Optimize predicate expression with asterisk and nested subquery
ANALYZE SELECT * FROM (SELECT * FROM (SELECT * FROM test.test)) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test)) WHERE id = 1;

-- Optimize predicate expression with qualified asterisk
ANALYZE SELECT * FROM (SELECT b.* FROM (SELECT * FROM test.test) AS b) WHERE id = 1;
SELECT * FROM (SELECT b.* FROM (SELECT * FROM test.test) AS b) WHERE id = 1;

-- Optimize predicate expression without asterisk
ANALYZE SELECT * FROM (SELECT date, id, name, value FROM test.test) WHERE id = 1;
SELECT * FROM (SELECT date, id, name, value FROM test.test) WHERE id = 1;

-- Optimize predicate expression without asterisk and contains nested subquery
ANALYZE SELECT * FROM (SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test.test)) WHERE id = 1;
SELECT * FROM (SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test.test)) WHERE id = 1;

-- Optimize predicate expression with qualified
ANALYZE SELECT * FROM (SELECT * FROM test.test) AS b WHERE b.id = 1;
SELECT * FROM (SELECT * FROM test.test) AS b WHERE b.id = 1;

-- Optimize predicate expression with qualified and nested subquery
ANALYZE SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) AS a) AS b WHERE b.id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) AS a) AS b WHERE b.id = 1;

-- Optimize predicate expression with aggregate function
ANALYZE SELECT * FROM (SELECT id, date, min(value) AS value FROM test.test GROUP BY id, date) WHERE id = 1;
SELECT * FROM (SELECT id, date, min(value) AS value FROM test.test GROUP BY id, date) WHERE id = 1;

-- Optimize predicate expression with union all query
ANALYZE SELECT * FROM (SELECT * FROM test.test UNION ALL SELECT * FROM test.test) WHERE id = 1;
SELECT * FROM (SELECT * FROM test.test UNION ALL SELECT * FROM test.test) WHERE id = 1;

-- Optimize predicate expression with join query
ANALYZE SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id WHERE id = 1;
SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id WHERE id = 1;

ANALYZE SELECT * FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test.test USING id WHERE value = 1;
SELECT * FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test.test USING id WHERE value = 1;

-- FIXME: no support for aliased tables for now.
ANALYZE SELECT b.value FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test.test AS b USING id WHERE value = 1;
SELECT b.value FROM (SELECT toInt8(1) AS id) ANY LEFT JOIN test.test AS b USING id WHERE value = 1;

-- Optimize predicate expression with join and nested subquery
ANALYZE SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id) WHERE id = 1;

-- Optimize predicate expression with join query and qualified
ANALYZE SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING id WHERE b.id = 1;
SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING id WHERE b.id = 1;

-- Compatibility test
ANALYZE SELECT * FROM (SELECT toInt8(1) AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING date, id WHERE b.date = toDate('2000-01-01');
SELECT * FROM (SELECT toInt8(1) AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING date, id WHERE b.date = toDate('2000-01-01');

ANALYZE SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) AS a ANY LEFT JOIN (SELECT * FROM test.test) AS b  ON  a.id = b.id) WHERE id = 1;
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) AS a ANY LEFT JOIN (SELECT * FROM test.test) AS b  ON  a.id = b.id) WHERE id = 1;

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;
