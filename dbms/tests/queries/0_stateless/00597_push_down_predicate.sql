SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;

CREATE TABLE test.test(date Date, id Int8, name String, value Int64) ENGINE = MergeTree(date, (id, date), 8192);
CREATE VIEW test.test_view AS SELECT * FROM test.test;

INSERT INTO test.test VALUES('2000-01-01', 1, 'test string 1', 1);
INSERT INTO test.test VALUES('2000-01-01', 2, 'test string 2', 2);

SET enable_optimize_predicate_expression = 1;

SELECT '-------Not need optimize predicate, but it works.-------';
SELECT 1;
SELECT 1 AS id WHERE id = 1;
SELECT arrayJoin([1,2,3]) AS id WHERE id = 1;

SELECT '-------Need push down-------';
SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2) WHERE id = 1;
SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;
SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;

SELECT * FROM (SELECT 1 AS id, (SELECT 1)) WHERE _subquery1  = 1;
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test) WHERE a = 3;
SELECT * FROM (SELECT toUInt64(b), sum(id) AS b FROM test.test) WHERE `toUInt64(sum(id))` = 3;
SELECT date, id, name, value FROM (SELECT date, name, value, min(id) AS id FROM test.test GROUP BY date, name, value) WHERE id = 1;
SELECT * FROM (SELECT toUInt64(table_alias.b) AS a, sum(id) AS b FROM test.test AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3;

SET force_primary_key = 1;

-- Optimize predicate expression with asterisk
SELECT * FROM (SELECT * FROM test.test) WHERE id = 1;
-- Optimize predicate expression with asterisk and nested subquery
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test)) WHERE id = 1;
-- Optimize predicate expression with qualified asterisk
SELECT * FROM (SELECT b.* FROM (SELECT * FROM test.test) AS b) WHERE id = 1;
-- Optimize predicate expression without asterisk
SELECT * FROM (SELECT date, id, name, value FROM test.test) WHERE id = 1;
-- Optimize predicate expression without asterisk and contains nested subquery
SELECT * FROM (SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test.test)) WHERE id = 1;
-- Optimize predicate expression with qualified
SELECT * FROM (SELECT * FROM test.test) AS b WHERE b.id = 1;
-- Optimize predicate expression with qualified and nested subquery
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) AS a) AS b WHERE b.id = 1;
-- Optimize predicate expression with aggregate function
SELECT * FROM (SELECT id, date, min(value) AS value FROM test.test GROUP BY id, date) WHERE id = 1;

-- Optimize predicate expression with union all query
SELECT * FROM (SELECT * FROM test.test UNION ALL SELECT * FROM test.test) WHERE id = 1;
-- Optimize predicate expression with join query
SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id WHERE id = 1;
-- Optimize predicate expression with join and nested subquery
SELECT * FROM (SELECT * FROM (SELECT * FROM test.test) ANY LEFT JOIN (SELECT * FROM test.test) USING id) WHERE id = 1;
-- Optimize predicate expression with join query and qualified
SELECT * FROM (SELECT 1 AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING date WHERE b.id = 1;

-- Optimize predicate expression with view
SELECT * FROM test.test_view WHERE id = 1;
SELECT * FROM test.test_view WHERE id = 2;
SELECT id FROM test.test_view WHERE id  = 1;
SELECT s.id FROM test.test_view AS s WHERE id = 1;

SELECT '-------Push to having expression, need check.-------';
SELECT id FROM (SELECT min(id) AS id FROM test.test) WHERE id = 1; -- { serverError 277 }
SELECT * FROM (SELECT toUInt64(b) AS a, sum(id) AS b FROM test.test) WHERE a = 3; -- { serverError 277 }
SELECT * FROM (SELECT toUInt64(b), sum(id) AS b FROM test.test) WHERE `toUInt64(sum(id))` = 3; -- { serverError 277 }
SELECT * FROM (SELECT toUInt64(table_alias.b) AS a, sum(id) AS b FROM test.test AS table_alias) AS outer_table_alias WHERE outer_table_alias.b = 3; -- { serverError 277 }

SELECT '-------Compatibility test-------';
SELECT * FROM (SELECT 1 AS id, toDate('2000-01-01') AS date FROM system.numbers LIMIT 1) ANY LEFT JOIN (SELECT * FROM test.test) AS b USING date WHERE b.date = toDate('2000-01-01'); -- {serverError 47}

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;
