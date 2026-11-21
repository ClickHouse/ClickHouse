-- Tags: no-fasttest
-- no-fasttest: requires ICU for collation support.

-- The threshold filter of a partial sort with LIMIT >= 1500 must compare rows with the collation.
-- Two blocks of 2000 rows: the first sets the threshold, the second holds rows that are smaller under 'en' but bytewise greater.
SET max_threads = 1, max_block_size = 2000;

SELECT 'asc', count(), min(s), max(s) FROM (SELECT s FROM (SELECT concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1500);
SELECT 'desc', count(), min(s), max(s) FROM (SELECT s FROM (SELECT concat(if(number < 2000, 'a', 'B'), leftPad(toString(number), 5, '0')) AS s FROM numbers(4000)) ORDER BY s DESC COLLATE 'en' LIMIT 1500);
SELECT 'offset', count(), min(s), max(s) FROM (SELECT s FROM (SELECT concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1000, 600);
SELECT 'plain_then_collated', count(), min(s), max(s) FROM (SELECT k, s FROM (SELECT intDiv(number, 1000000) AS k, concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')) AS s FROM numbers(4000)) ORDER BY k, s COLLATE 'en' LIMIT 1500);
SELECT 'collated_then_plain', count(), min(s), max(s) FROM (SELECT s, n FROM (SELECT number AS n, concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en', n LIMIT 1500);
SELECT 'collated_ties', count(), countIf(s = 'a'), minIf(n, s = 'B'), maxIf(n, s = 'B') FROM (SELECT s, n FROM (SELECT if(number < 2000 OR number >= 3000, 'B', 'a') AS s, if(number < 2000, number + 1000, if(number < 3000, number, number % 1000)) AS n FROM numbers(4000)) ORDER BY s COLLATE 'en', n LIMIT 1500);
SELECT 'nullable', count(), countIf(s IS NULL), min(s), max(s) FROM (SELECT s FROM (SELECT if(number >= 3900, NULL, toNullable(concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')))) AS s FROM numbers(4000)) ORDER BY s NULLS FIRST COLLATE 'en' LIMIT 1500);
SELECT 'low_cardinality', count(), min(s), max(s) FROM (SELECT s FROM (SELECT toLowCardinality(concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0'))) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1500);
SELECT 'low_cardinality_nullable', count(), min(s), max(s) FROM (SELECT s FROM (SELECT toLowCardinality(toNullable(concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0')))) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1500);
SELECT 'array', count(), min(s[1]), max(s[1]) FROM (SELECT s FROM (SELECT [concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0'))] AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1500);
SELECT 'tuple', count(), min(s.1), max(s.1) FROM (SELECT s FROM (SELECT tuple(concat(if(number < 2000, 'B', 'a'), leftPad(toString(number), 5, '0'))) AS s FROM numbers(4000)) ORDER BY s COLLATE 'en' LIMIT 1500);
