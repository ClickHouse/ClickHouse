-- Tags: distributed

-- { echo }

SET enable_analyzer=0;

SELECT 'Old Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number DESC LIMIT 20, 5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -20, -5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -20, 5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT 20, -5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x DESC LIMIT 20, 5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT -20, -5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT -20, 5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT 20, -5 WITH TIES;


SET enable_analyzer=1;

SELECT 'Analyzer:';

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number DESC LIMIT 20, 5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -20, -5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT -20, 5 WITH TIES;

SELECT number FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY number LIMIT 20, -5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x DESC LIMIT 20, 5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT -20, -5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT -20, 5 WITH TIES;

SELECT intDiv(number, 3) AS x FROM remote('127.0.0.{1,2,3}', numbers_mt(20)) ORDER BY x LIMIT 20, -5 WITH TIES;
