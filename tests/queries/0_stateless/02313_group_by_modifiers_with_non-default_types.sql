DROP TABLE IF EXISTS test02313;

CREATE TABLE test02313
(
    a Enum('one' = 1, 'two' = 2),
    b Enum('default' = 0, 'non-default' = 1),
    c UInt8
)
ENGINE = MergeTree()
ORDER BY (a, b, c);

INSERT INTO test02313 SELECT number % 2 + 1 AS a, number % 2 AS b, number FROM numbers(10);

-- { echoOn }
SELECT
    count() as d, a, b, c
FROM test02313
GROUP BY ROLLUP(a, b, c)
ORDER BY d, a, b, c;

SELECT
    count() as d, a, b, c
FROM test02313
GROUP BY CUBE(a, b, c)
ORDER BY d, a, b, c;

SELECT
    count() as d, a, b, c
FROM test02313
GROUP BY GROUPING SETS
    (
        (c),
        (a, c),
        (b, c)
    )
ORDER BY d, a, b, c;

-- { echoOff }
DROP TABLE test02313;
