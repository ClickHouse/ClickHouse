-- { echo }

SELECT 'Large mt, last 1 with ties';
SELECT intDiv(number, 2) AS x FROM numbers_mt(1000000) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Large mt, intDiv 500000';
SELECT intDiv(number, 500000) AS x FROM numbers_mt(1000001) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Zero limit with ties';
SELECT 0 AS x FROM numbers(100) ORDER BY x LIMIT -0 WITH TIES;

SELECT 'All same value, large';
SELECT count() FROM (SELECT 0 AS x FROM numbers_mt(100000) ORDER BY x LIMIT -1 WITH TIES);

SET max_block_size = 2;
SELECT 'Small blocks, large data';
SELECT intDiv(number, 2) AS x FROM numbers(1000) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Small blocks, all same';
SELECT count() FROM (SELECT 0 AS x FROM numbers(100) ORDER BY x LIMIT -3 WITH TIES);
