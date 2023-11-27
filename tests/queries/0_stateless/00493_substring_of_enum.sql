DROP TABLE IF EXISTS substring_enums_test;
CREATE TABLE substring_enums_test(e8 Enum('hello' = -5, 'world' = 15), e16 Enum('shark' = -999, 'eagle' = 9999)) ENGINE MergeTree ORDER BY tuple();
INSERT INTO TABLE substring_enums_test VALUES ('hello', 'shark'), ('world', 'eagle');

SELECT '-- Positive offsets (slice from left)';
WITH cte AS (SELECT number + 1 AS n FROM system.numbers LIMIT 6),
     permutations AS (SELECT c1.n AS offset, c2.n AS length FROM cte AS c1 CROSS JOIN cte AS c2)
SELECT 'Offset: ', p.offset, 'Length: ', p.length,
       substring(e8, p.offset) AS s1, substring(e16, p.offset) AS s2,
       substring(e8, p.offset, p.length) AS s3, substring(e16, p.offset, p.length) AS s4
FROM substring_enums_test LEFT JOIN permutations AS p ON true;

SELECT '-- Negative offsets (slice from right)';
WITH cte AS (SELECT number + 1 AS n FROM system.numbers LIMIT 6),
     permutations AS (SELECT -c1.n AS offset, c2.n AS length FROM cte AS c1 CROSS JOIN cte AS c2)
SELECT 'Offset: ', p.offset, 'Length: ', p.length,
       substring(e8, p.offset) AS s1, substring(e16, p.offset) AS s2,
       substring(e8, p.offset, p.length) AS s3, substring(e16, p.offset, p.length) AS s4
FROM substring_enums_test LEFT JOIN permutations AS p ON true;

SELECT '-- Zero offset/length';
WITH cte AS (SELECT number AS n FROM system.numbers LIMIT 2),
     permutations AS (SELECT c1.n AS offset, c2.n AS length FROM cte AS c1 CROSS JOIN cte AS c2 LIMIT 3)
SELECT 'Offset: ', p.offset, 'Length: ', p.length,
       substring(e8, p.offset) AS s1, substring(e16, p.offset) AS s2,
       substring(e8, p.offset, p.length) AS s3, substring(e16, p.offset, p.length) AS s4
FROM substring_enums_test LEFT JOIN permutations AS p ON true;

SELECT '-- Constant enums';
SELECT substring(CAST('foo', 'Enum8(\'foo\' = 1)'), 1, 1), substring(CAST('foo', 'Enum16(\'foo\' = 1111)'), 1, 2);

DROP TABLE substring_enums_test;
