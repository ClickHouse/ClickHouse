-- Tags: distributed

-- { echo }

SET max_block_size = 1;

SET enable_analyzer = 0;

SELECT 'Old Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 3 AFTER number >= 3;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 2 AFTER number IN (2, 3, 6) ALL;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7;

SET enable_analyzer = 1;

SELECT 'Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 3 AFTER number >= 3;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 2 AFTER number IN (2, 3, 6) ALL;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7;


SET max_block_size = 4;

SET enable_analyzer = 0;

SELECT 'Old Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 3 AFTER number >= 3;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 2 AFTER number IN (2, 3, 6) ALL;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7;

SET enable_analyzer = 1;

SELECT 'Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 3 AFTER number >= 3;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 10 AFTER number >= 2 UNTIL number >= 6;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT 2 AFTER number IN (2, 3, 6) ALL;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(10)) ORDER BY number LIMIT AFTER number >= 7;
