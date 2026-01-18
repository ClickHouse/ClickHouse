SET enable_analyzer=0;

SELECT 'Old Analyzer';

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 3 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 297;

SET enable_analyzer=1;

SELECT 'New Analyzer';

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 3 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 297;