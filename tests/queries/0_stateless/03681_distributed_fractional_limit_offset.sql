SET enable_analyzer=0;

SELECT 'Old Analyzer';

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 0.01;

SELECT number from remote('127.0.0.1', numbers_mt(100)) OFFSET 0.99;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 1 OFFSET 0.9;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 0.01 OFFSET 90;

SET enable_analyzer=1;

SELECT 'New Analyzer';

SELECT number from remote('127.0.0.1', numbers_mt(100)) ORDER BY number LIMIT 0.01;

SELECT number from remote('127.0.0.1', numbers_mt(100)) OFFSET 0.99;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 1 OFFSET 0.9;

SELECT number from remote('127.0.0.1', numbers_mt(100)) LIMIT 0.01 OFFSET 90;