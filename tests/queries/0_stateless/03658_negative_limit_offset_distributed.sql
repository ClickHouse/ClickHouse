-- Tags: distributed

SET enable_analyzer=0;

SELECT 'Old Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number DESC LIMIT 5 OFFSET 20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -5 OFFSET -20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT 5 OFFSET -20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -5 OFFSET 20;


SET enable_analyzer=1;

SELECT 'New Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number DESC LIMIT 5 OFFSET 20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -5 OFFSET -20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT 5 OFFSET -20;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -5 OFFSET 20;
