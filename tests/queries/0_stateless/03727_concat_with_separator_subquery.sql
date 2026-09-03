-- { echoOn}

SELECT concatWithSeparator(c0, 'b', 1) FROM (SELECT 'a' c0) tx;

SELECT concatWithSeparator(c0, 1) FROM (SELECT 'a' AS c0) tx;

SELECT concatWithSeparator(c0, 'b', 1) FROM (SELECT CAST(NULL AS Nullable(String)) AS c0) tx;

SELECT concatWithSeparator('a', c1, 'b') FROM (SELECT 1 AS c1) tx;

SELECT concatWithSeparator('a', c1, 'b') FROM (SELECT CAST(1 AS Nullable(UInt8)) AS c1) tx;

SELECT concatWithSeparator('+-*/', c0, c1, c2) FROM (SELECT 'a' AS c0) t0 CROSS JOIN (SELECT 'b' AS c1) t1 CROSS JOIN (SELECT 1 AS c2) t2;

SELECT concatWithSeparator(c0, c1, c2)
FROM
(
    SELECT
        'a' AS c0,
        toString(number) AS c1,
        number + 9 AS c2
    FROM numbers(5)
);

SELECT concatWithSeparator('-', [1, 2, 3], [4, 5, 6]) FROM numbers(3);

SELECT concatWithSeparator('-', (1, 'a'), (2, 'b')) FROM numbers(3);

SELECT concatWithSeparator(c0, c1, c2)
FROM
(
    SELECT
        '+' AS c0,
        (toString(number), number + 1) AS c1,
        [number + 9, number + 3, number + 2] AS c2
    FROM numbers(5)
);

SELECT concatWithSeparator('+', c1, c2)
FROM
(
    SELECT
        (toString(number), number + 1) AS c1,
        [number + 9, number + 3, number + 2] AS c2
    FROM numbers(5)
);


SELECT concatWithSeparator('+', c1, c2, 'zz')
FROM
(
    SELECT
        (toString(number), number + 1) AS c1,
        [number + 9, number + 3, number + 2] AS c2
    FROM numbers(5)
);
