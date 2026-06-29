SET enable_analyzer = 1;

SET optimize_empty_string_comparisons = 0;

DROP TABLE IF EXISTS 02952_disjunction_optimization;

CREATE TABLE 02952_disjunction_optimization
(a Int32, b String)
ENGINE=Memory;

INSERT INTO 02952_disjunction_optimization VALUES (1, 'test'), (2, 'test2'), (3, 'another'), (3, ''), (4, '');

SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4;
EXPLAIN QUERY TREE SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4;

SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4 AND true;
EXPLAIN QUERY TREE SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4 AND true;

SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4 AND b <> '';
EXPLAIN QUERY TREE SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND a <> 4 AND b <> '';

SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND b = '' AND a <> 4;
EXPLAIN QUERY TREE SELECT * FROM 02952_disjunction_optimization WHERE a <> 1 AND a <> 2 AND b = '' AND a <> 4;

SELECT * FROM 02952_disjunction_optimization WHERE (a <> 1 AND a <> 2 AND a <> 4) OR b = '';
EXPLAIN QUERY TREE SELECT * FROM 02952_disjunction_optimization WHERE (a <> 1 AND a <> 2 AND a <> 4) OR b = '';

DROP TABLE 02952_disjunction_optimization;
