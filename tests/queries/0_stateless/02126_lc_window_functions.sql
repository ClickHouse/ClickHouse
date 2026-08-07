SELECT max(id) OVER () AS aid
FROM
(
    SELECT materialize(toLowCardinality('aaaa')) AS id
    FROM numbers_mt(1000000)
)
FORMAT `Null`;

SELECT max(id) OVER (PARTITION BY id) AS id
FROM
(
    SELECT materialize('aaaa') AS id
    FROM numbers_mt(1000000)
)
FORMAT `Null`;

SELECT countIf(sym = 'Red') OVER () AS res
FROM
(
    SELECT CAST(CAST(number % 5, 'Enum8(\'Red\' = 0, \'Blue\' = 1, \'Yellow\' = 2, \'Black\' = 3, \'White\' = 4)'), 'LowCardinality(String)') AS sym
    FROM numbers(10)
);

SELECT materialize(toLowCardinality('a\0aa')), countIf(toLowCardinality('aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0aaaaaaa\0'), sym = 'Red') OVER (Range BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS res FROM (SELECT CAST(CAST(number % 5, 'Enum8(\'Red\' = 0, \'Blue\' = 1, \'Yellow\' = 2, \'Black\' = 3, \'White\' = 4)'), 'LowCardinality(String)') AS sym FROM numbers(3));

SELECT
    NULL,
    id,
    max(id) OVER (Rows BETWEEN 10 PRECEDING AND UNBOUNDED FOLLOWING) AS aid
FROM
(
    SELECT
        NULL,
        max(id) OVER (),
        materialize(toLowCardinality('')) AS id
    FROM numbers_mt(0, 1)
)
FORMAT `Null`;
