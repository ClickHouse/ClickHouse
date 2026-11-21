SET enable_analyzer=0;

EXPLAIN SELECT * FROM numbers(100) ORDER BY number LIMIT 10 OFFSET 90;

EXPLAIN SELECT * FROM remote('127.0.0.{2,3}', numbers(100)) ORDER BY number LIMIT 1;

SET enable_analyzer=1;

EXPLAIN SELECT * FROM numbers(100) ORDER BY number LIMIT 10 OFFSET 90;

EXPLAIN SELECT * FROM remote('127.0.0.{2,3}', numbers(100)) ORDER BY number LIMIT 1;