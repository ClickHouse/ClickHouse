-- Exercises skipping of non-matching key runs in full sorting merge join
-- (a single first-key-column track skips a whole run of rows that are less
-- than the current row of the other side).

SET join_algorithm = 'full_sorting_merge';
-- Small blocks to get key runs crossing block boundaries.
SET max_block_size = 113;

SELECT '-- inner, dense left with duplicates vs sparse right';
SELECT count(), sum(l.k), sum(r.k), sum(l.v), sum(r.v)
FROM (SELECT intDiv(number, 3) AS k, number AS v FROM numbers(10000)) AS l
INNER JOIN (SELECT number * 10 AS k, number AS v FROM numbers(500)) AS r
ON l.k = r.k;

SELECT '-- inner, sparse left vs dense right with duplicates';
SELECT count(), sum(l.k), sum(r.k), sum(l.v), sum(r.v)
FROM (SELECT number * 10 AS k, number AS v FROM numbers(500)) AS l
INNER JOIN (SELECT intDiv(number, 3) AS k, number AS v FROM numbers(10000)) AS r
ON l.k = r.k;

SELECT '-- left, sparse right';
SELECT count(), sum(l.k), sum(r.k), sum(l.v), sum(r.v), countIf(r.k = 0)
FROM (SELECT number AS k, number AS v FROM numbers(10000)) AS l
LEFT JOIN (SELECT number * 100 AS k, number AS v FROM numbers(50)) AS r
ON l.k = r.k;

SELECT '-- right, sparse left';
SELECT count(), sum(l.k), sum(r.k), sum(l.v), sum(r.v), countIf(l.k = 0)
FROM (SELECT number * 100 AS k, number AS v FROM numbers(50)) AS l
RIGHT JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS r
ON l.k = r.k;

SELECT '-- full, interleaved multiples of 2 and 3';
SELECT count(), sum(l.k), sum(r.k), countIf(l.v = 0 AND l.k = 0), countIf(r.v = 0 AND r.k = 0)
FROM (SELECT number * 2 AS k, number + 1 AS v FROM numbers(5000)) AS l
FULL JOIN (SELECT number * 3 AS k, number + 1 AS v FROM numbers(5000)) AS r
ON l.k = r.k;

SELECT '-- any inner, duplicates on both sides';
SELECT count(), sum(l.k), sum(r.k)
FROM (SELECT intDiv(number, 3) AS k FROM numbers(10000)) AS l
ANY INNER JOIN (SELECT intDiv(number, 2) * 10 AS k FROM numbers(1000)) AS r
ON l.k = r.k;

SELECT '-- any left, sparse right with distinct keys';
SELECT count(), sum(l.k), sum(l.v), sum(r.k), sum(r.v)
FROM (SELECT intDiv(number, 3) AS k, number AS v FROM numbers(10000)) AS l
ANY LEFT JOIN (SELECT number * 10 AS k, number AS v FROM numbers(500)) AS r
ON l.k = r.k;

SELECT '-- any right, sparse left with distinct keys';
SELECT count(), sum(l.k), sum(l.v), sum(r.k), sum(r.v)
FROM (SELECT number * 10 AS k, number AS v FROM numbers(500)) AS l
ANY RIGHT JOIN (SELECT intDiv(number, 3) AS k, number AS v FROM numbers(10000)) AS r
ON l.k = r.k;

SELECT '-- inner, multi-column key, first column with duplicate runs';
SELECT count(), sum(l.k1), sum(l.k2), sum(r.k1), sum(r.k2)
FROM (SELECT intDiv(number, 100) AS k1, number % 100 AS k2 FROM numbers(10000)) AS l
INNER JOIN (SELECT intDiv(number * 7, 100) AS k1, (number * 7) % 100 AS k2 FROM numbers(1000)) AS r
ON l.k1 = r.k1 AND l.k2 = r.k2;

SELECT '-- inner, nullable keys, nulls never match';
SELECT count(), sum(l.k), sum(r.k), countIf(l.k IS NULL), countIf(r.k IS NULL)
FROM (SELECT if(number % 7 = 0, NULL, number) AS k FROM numbers(3000)) AS l
INNER JOIN (SELECT if(number % 5 = 0, NULL, number * 4) AS k FROM numbers(700)) AS r
ON l.k = r.k;

SELECT '-- left, nullable keys';
SELECT count(), sum(l.k), sum(r.k), countIf(l.k IS NULL), countIf(r.k IS NULL)
FROM (SELECT if(number % 7 = 0, NULL, number) AS k FROM numbers(3000)) AS l
LEFT JOIN (SELECT if(number % 5 = 0, NULL, number * 4) AS k FROM numbers(700)) AS r
ON l.k = r.k
SETTINGS join_use_nulls = 1;

SELECT '-- inner, string keys';
SELECT count(), sum(length(l.k)), sum(length(r.k))
FROM (SELECT leftPad(toString(number), 8, '0') AS k FROM numbers(10000)) AS l
INNER JOIN (SELECT leftPad(toString(number * 10), 8, '0') AS k FROM numbers(500)) AS r
ON l.k = r.k;

SELECT '-- inner, decimal keys';
SELECT count(), sum(l.k), sum(r.k)
FROM (SELECT toDecimal64(number, 3) AS k FROM numbers(10000)) AS l
INNER JOIN (SELECT toDecimal64(number * 10, 3) AS k FROM numbers(500)) AS r
ON l.k = r.k;

SELECT '-- inner, fixed string keys';
SELECT count(), sum(length(l.k)), sum(length(r.k))
FROM (SELECT toFixedString(leftPad(toString(number), 8, '0'), 8) AS k FROM numbers(10000)) AS l
INNER JOIN (SELECT toFixedString(leftPad(toString(number * 10), 8, '0'), 8) AS k FROM numbers(500)) AS r
ON l.k = r.k;

SELECT '-- disjoint key ranges, no matches at all';
SELECT count(), sum(l.k), sum(r.k)
FROM (SELECT number AS k FROM numbers(5000)) AS l
FULL JOIN (SELECT number + 1000000 AS k FROM numbers(5000)) AS r
ON l.k = r.k;

-- Float64 first key with +-Inf / NaN through trackCursorsFirstKey. The run 7,8,9 < 10
-- forces a multi-row track; NaN is present on BOTH sides so the first-key comparison
-- sees NaN vs NaN, and the mismatching second key keeps the pair from joining, without
-- pinning down whether NaN keys with equal remaining keys would match.
SELECT '-- inner, float keys with inf and nan, multi-column';
SELECT l.k1, l.k2, l.val, r.val
FROM (SELECT * FROM values('k1 Float64, k2 UInt32, val String',
    (-inf, 0, 'L-inf'), (1, 0, 'L1'), (2, 0, 'L2'), (3, 0, 'L3'), (7, 0, 'L7'), (8, 0, 'L8'), (9, 0, 'L9'), (inf, 0, 'Linf'), (nan, 10, 'Lnan10'), (nan, 20, 'Lnan20'))) AS l
INNER JOIN (SELECT * FROM values('k1 Float64, k2 UInt32, val String',
    (0, 0, 'R0'), (3, 0, 'R3'), (5, 0, 'R5'), (10, 0, 'R10'), (inf, 0, 'Rinf'), (nan, 30, 'Rnan30'))) AS r
ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY isNaN(l.k1), l.k1, l.k2, l.val;

SELECT '-- left, float keys with inf and nan, multi-column';
SELECT l.k1, l.k2, l.val, r.val
FROM (SELECT * FROM values('k1 Float64, k2 UInt32, val String',
    (-inf, 0, 'L-inf'), (1, 0, 'L1'), (2, 0, 'L2'), (3, 0, 'L3'), (7, 0, 'L7'), (8, 0, 'L8'), (9, 0, 'L9'), (inf, 0, 'Linf'), (nan, 10, 'Lnan10'), (nan, 20, 'Lnan20'))) AS l
LEFT JOIN (SELECT * FROM values('k1 Float64, k2 UInt32, val String',
    (0, 0, 'R0'), (3, 0, 'R3'), (5, 0, 'R5'), (10, 0, 'R10'), (inf, 0, 'Rinf'), (nan, 30, 'Rnan30'))) AS r
ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY isNaN(l.k1), l.k1, l.k2, l.val;

SELECT '-- nan first keys with mismatching second key stay unmatched';
SELECT count()
FROM (SELECT * FROM values('k1 Float64, k2 UInt32',
    (-inf, 0), (1, 0), (2, 0), (3, 0), (7, 0), (8, 0), (9, 0), (inf, 0), (nan, 10), (nan, 20))) AS l
INNER JOIN (SELECT * FROM values('k1 Float64, k2 UInt32',
    (0, 0), (3, 0), (5, 0), (10, 0), (inf, 0), (nan, 30))) AS r
ON l.k1 = r.k1 AND l.k2 = r.k2
WHERE isNaN(l.k1);
