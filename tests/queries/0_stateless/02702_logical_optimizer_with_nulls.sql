SET enable_analyzer = 1;

DROP TABLE IF EXISTS 02702_logical_optimizer;

CREATE TABLE 02702_logical_optimizer
(a Int32, b LowCardinality(String))
ENGINE=Memory;

INSERT INTO 02702_logical_optimizer VALUES (1, 'test'), (2, 'test2'), (3, 'another');

SET optimize_min_equality_disjunction_chain_length = 3;

SELECT * FROM 02702_logical_optimizer WHERE a = 1 OR 3 = a OR NULL = a;
EXPLAIN QUERY TREE SELECT * FROM 02702_logical_optimizer WHERE a = 1 OR 3 = a OR NULL = a;

SELECT * FROM 02702_logical_optimizer WHERE a = 1 OR 3 = a OR 2 = a OR a = NULL;
EXPLAIN QUERY TREE SELECT * FROM 02702_logical_optimizer WHERE a = 1 OR 3 = a OR 2 = a OR a = NULL;

DROP TABLE 02702_logical_optimizer;

DROP TABLE IF EXISTS 02702_logical_optimizer_with_null_column;

CREATE TABLE 02702_logical_optimizer_with_null_column
(a Nullable(Int32), b LowCardinality(String))
ENGINE=Memory;

INSERT INTO 02702_logical_optimizer_with_null_column VALUES (1, 'test'), (2, 'test2'), (3, 'another');

SELECT * FROM 02702_logical_optimizer_with_null_column WHERE a = 1 OR 3 = a OR 2 = a;
EXPLAIN QUERY TREE SELECT * FROM 02702_logical_optimizer_with_null_column WHERE a = 1 OR 3 = a OR 2 = a;

SELECT materialize(1) AS k WHERE NULL OR (0 OR (k = 2) OR (k = CAST(1, 'Nullable(UInt8)') OR k = 3));
SELECT (k = 2) OR (k = 1) OR ((NULL OR 1) = k) FROM (SELECT 1 AS k);

DROP TABLE 02702_logical_optimizer_with_null_column;
