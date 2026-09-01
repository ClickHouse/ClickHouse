-- Tags: distributed

-- { echo }

SET max_block_size = 1;

SET enable_analyzer = 0;

SELECT 'Old Analyzer:';

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -4 OFFSET -3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 OFFSET 3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;

SET enable_analyzer = 1;

SELECT 'Analyzer:';

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -4 OFFSET -3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 OFFSET 3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;


SET max_block_size = 4;

SET enable_analyzer = 0;

SELECT 'Old Analyzer:';

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -4 OFFSET -3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 OFFSET 3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;

SET enable_analyzer = 1;

SELECT 'Analyzer:';

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -4 OFFSET -3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT -2 OFFSET 3 BY g;

SELECT number, number % 3 AS g FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;
