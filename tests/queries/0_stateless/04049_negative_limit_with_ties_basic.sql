-- { echo }

SELECT 'Basic: ties at boundary';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;

SELECT 'No ties at boundary';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -2 WITH TIES;

SELECT 'Last 1 with ties';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Limit exceeds total';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -100 WITH TIES;

SELECT 'Negative limit + positive offset';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT 4, -3 WITH TIES;

SELECT 'Negative limit + negative offset';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, -2 WITH TIES;

SELECT 'All same values';
SELECT 0 AS x FROM numbers(5) ORDER BY x LIMIT -2 WITH TIES;

SELECT 'Ties extend to all';
SELECT 0 AS x FROM numbers(5) ORDER BY x LIMIT -1 WITH TIES;

SELECT 'Multiple ORDER BY columns, no ties';
SELECT intDiv(number, 2) AS x, number AS y FROM numbers(10) ORDER BY x, y LIMIT -3 WITH TIES;

SET max_block_size = 2;
SELECT 'Small block size';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3 WITH TIES;

SET max_block_size = 65536;
SELECT 'Positive limit + negative offset';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3, 3 WITH TIES;

SELECT 'Zero limit';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -0 WITH TIES;

SELECT 'Without ties sanity check';
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x LIMIT -3;

SELECT 'Distinct values, no ties';
SELECT number FROM numbers(8) ORDER BY number LIMIT -3 WITH TIES;

SELECT 'Modulo groups, ties at boundary';
SELECT number % 3 AS x FROM numbers(9) ORDER BY x LIMIT -4 WITH TIES;

SELECT 'Modulo groups, positive offset + negative limit';
SELECT number % 3 AS x FROM numbers(9) ORDER BY x LIMIT 2, -3 WITH TIES;

SELECT 'Modulo groups, both negative';
SELECT number % 3 AS x FROM numbers(9) ORDER BY x LIMIT -2, -3 WITH TIES;
