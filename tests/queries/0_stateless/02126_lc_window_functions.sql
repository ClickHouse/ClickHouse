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
