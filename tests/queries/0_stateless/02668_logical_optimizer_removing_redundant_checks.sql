SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS 02668_logical_optimizer;

CREATE TABLE 02668_logical_optimizer
(a Int32, b LowCardinality(String))
ENGINE=Memory;

INSERT INTO 02668_logical_optimizer VALUES (1, 'test'), (2, 'test2'), (3, 'another');

SET optimize_min_equality_disjunction_chain_length = 2;

SELECT * FROM 02668_logical_optimizer WHERE a = 1 OR 3 = a OR 1 = a;
EXPLAIN QUERY TREE SELECT * FROM 02668_logical_optimizer WHERE a = 1 OR 3 = a OR 1 = a;

SELECT * FROM 02668_logical_optimizer WHERE a = 1 OR 1 = a;
EXPLAIN QUERY TREE SELECT * FROM 02668_logical_optimizer WHERE a = 1 OR 1 = a;

SELECT * FROM 02668_logical_optimizer WHERE a = 1 AND 2 = a;
EXPLAIN QUERY TREE SELECT * FROM 02668_logical_optimizer WHERE a = 1 AND 2 = a;

SELECT * FROM 02668_logical_optimizer WHERE 3 = a AND b = 'another' AND a = 3;
EXPLAIN QUERY TREE SELECT * FROM 02668_logical_optimizer WHERE a = 3 AND b = 'another' AND a = 3;

SELECT * FROM 02668_logical_optimizer WHERE a = 2 AND 2 = a;
EXPLAIN QUERY TREE SELECT * FROM 02668_logical_optimizer WHERE a = 2 AND 2 = a;
