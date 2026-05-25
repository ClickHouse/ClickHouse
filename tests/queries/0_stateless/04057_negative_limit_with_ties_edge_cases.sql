-- Tags: no-fasttest
-- no-fasttest: Collations support is disabled

-- { echo }

SELECT 'DESC order, ties crossing chunk boundary';
SET max_block_size = 3;
SELECT number FROM numbers(10) ORDER BY number DESC LIMIT -4 WITH TIES;

SELECT 'DESC order, ties at boundary';
SET max_block_size = 3;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x DESC LIMIT -3 WITH TIES;

SELECT 'All rows equal';
SET max_block_size = 65536;
SELECT count() FROM (SELECT 0 AS x FROM numbers(100) ORDER BY x LIMIT -5 WITH TIES);

SELECT 'Limit = 0, nonzero offset';
SELECT number FROM numbers(10) ORDER BY number LIMIT -3, 0 WITH TIES;

SELECT 'Tie-run starting inside a chunk';
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -5 WITH TIES;

SELECT 'Tie-run starting inside chunk, with extension';
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -4 WITH TIES;

SELECT 'Offset crossing chunk boundaries';
SET max_block_size = 3;
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -3, -4 WITH TIES;

SELECT 'Large chunk, non-ties, Step I and final partial push';
SET max_block_size = 65536;
SELECT min(number), max(number), count() FROM (
    SELECT number FROM numbers(1000) ORDER BY number LIMIT -100, -200
);

SELECT 'Collation-based ties on strings';
SELECT x FROM
(
    SELECT arrayJoin(['a', 'A', 'b', 'B', 'c', 'C']) AS x
)
ORDER BY x COLLATE 'en' LIMIT -2 WITH TIES;

SELECT 'DESC with small blocks and ties';
SET max_block_size = 2;
SELECT intDiv(number, 2) AS x FROM numbers(10) ORDER BY x DESC LIMIT -5 WITH TIES;

SELECT 'Multiple sort columns';
SET max_block_size = 65536;
SELECT intDiv(number, 3) AS x, number % 2 AS y FROM numbers(12) ORDER BY x, y LIMIT -4 WITH TIES;

SELECT 'NULLs in sort key';
SELECT x FROM (SELECT if(number % 3 = 0, NULL, number) AS x FROM numbers(10)) ORDER BY x LIMIT -3 WITH TIES;

SELECT 'NULLs in sort key, NULLS FIRST';
SELECT x FROM (SELECT if(number % 3 = 0, NULL, number) AS x FROM numbers(10)) ORDER BY x NULLS FIRST LIMIT -4 WITH TIES;

SELECT 'Single-element chunks';
SET max_block_size = 1;
SELECT intDiv(number, 3) AS x FROM numbers(9) ORDER BY x LIMIT -4 WITH TIES;

SELECT 'Boundary at chunk start';
SET max_block_size = 3;
SELECT intDiv(number, 3) AS x FROM numbers(9) ORDER BY x LIMIT -3 WITH TIES;

SELECT 'Tie-run covers window but not all data';
SET max_block_size = 65536;
SELECT x FROM (SELECT arrayJoin([0, 1, 2, 2, 2, 2, 2, 2, 3, 4]) AS x) ORDER BY x LIMIT -6 WITH TIES;

SELECT 'Single row';
SELECT number FROM numbers(1) ORDER BY number LIMIT -1 WITH TIES;

SELECT 'Limit exceeds total rows';
SELECT number FROM numbers(5) ORDER BY number LIMIT -100 WITH TIES;

SELECT 'Empty result';
SELECT number FROM numbers(0) ORDER BY number LIMIT -1 WITH TIES;

SELECT 'Offset discarding tied tail';
SELECT intDiv(number, 3) AS x FROM numbers(12) ORDER BY x LIMIT -3, -5 WITH TIES;

SELECT 'Case-insensitive collation, bytewise ties (matches positive LIMIT WITH TIES)';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin(['a', 'A']) AS x) ORDER BY x COLLATE 'en-u-ks-level1' LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin(['a', 'A']) AS x) ORDER BY x COLLATE 'en-u-ks-level1' LIMIT -1 WITH TIES);

SELECT 'Positive vs negative LIMIT WITH TIES symmetry under collation';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin(['a', 'A']) AS x) ORDER BY x COLLATE 'en-u-ks-level1' LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin(['a', 'A']) AS x) ORDER BY x DESC COLLATE 'en-u-ks-level1' LIMIT -1 WITH TIES);
