SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Scalar subqueries';

SELECT (SELECT 1);
WITH 1 AS a SELECT (SELECT a);

SELECT (SELECT id FROM test_table);
SELECT (SELECT value FROM test_table);
SELECT (SELECT id, value FROM test_table);

SELECT 'Subqueries FROM section';

SELECT a FROM (SELECT 1 AS a) AS b;
SELECT b.a FROM (SELECT 1 AS a) AS b;

SELECT a FROM (SELECT 1 AS a) AS b;
SELECT b.a FROM (SELECT 1 AS a) AS b;

WITH 1 AS global_a SELECT a FROM (SELECT global_a AS a) AS b;
WITH 1 AS global_a SELECT b.a FROM (SELECT global_a AS a) AS b;

SELECT * FROM (SELECT * FROM (SELECT * FROM test_table));
SELECT * FROM (SELECT id, value FROM (SELECT * FROM test_table));

WITH 1 AS a SELECT (SELECT * FROM (SELECT * FROM (SELECT a + 1)));

SELECT 'Subqueries CTE';

WITH subquery AS (SELECT 1 AS a) SELECT * FROM subquery;
WITH subquery AS (SELECT 1 AS a) SELECT a FROM subquery;
WITH subquery AS (SELECT 1 AS a) SELECT subquery.a FROM subquery;
WITH subquery AS (SELECT 1 AS a) SELECT subquery.* FROM subquery;
WITH subquery AS (SELECT 1 AS a) SELECT subquery.* APPLY toString FROM subquery;
WITH subquery AS (SELECT 1 AS a) SELECT subquery_alias.a FROM subquery AS subquery_alias;
WITH subquery AS (SELECT 1 AS a) SELECT subquery_alias.* FROM subquery AS subquery_alias;
WITH subquery AS (SELECT 1 AS a) SELECT subquery_alias.* APPLY toString FROM subquery AS subquery_alias;

WITH subquery_1 AS (SELECT 1 AS a), subquery_2 AS (SELECT 1 + subquery_1.a FROM subquery_1) SELECT * FROM subquery_2;
WITH subquery_1 AS (SELECT 1 AS a), subquery_2 AS (SELECT (1 + subquery_1.a) AS a FROM subquery_1) SELECT subquery_2.a FROM subquery_2;

DROP TABLE test_table;
