-- https://github.com/ClickHouse/ClickHouse/issues/70769

SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 0;

SELECT 'tuple alias with CUBE', count()
FROM
(
    WITH (a, b) AS t
    SELECT t.1, t.2, count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY t WITH CUBE
);

SELECT 'tuple alias with ROLLUP', count()
FROM
(
    WITH (a, b) AS t
    SELECT t.1, t.2, count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY t WITH ROLLUP
);

SELECT 'tuple function with CUBE', count()
FROM
(
    SELECT tuple(a, b), count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY tuple(a, b) WITH CUBE
);

SELECT 'tuple function with ROLLUP', count()
FROM
(
    SELECT tuple(a, b), count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY tuple(a, b) WITH ROLLUP
);

SELECT 'tuple key with scalar and CUBE', count()
FROM
(
    SELECT (a, b).1, (a, b).2, c, count()
    FROM
    (
        SELECT number AS a, toString(number) AS b, number AS c
        FROM numbers(3)
    )
    GROUP BY (a, b), c WITH CUBE
);

SELECT 'tuple key with scalar and ROLLUP', count()
FROM
(
    SELECT (a, b).1, (a, b).2, c, count()
    FROM
    (
        SELECT number AS a, toString(number) AS b, number AS c
        FROM numbers(3)
    )
    GROUP BY (a, b), c WITH ROLLUP
);

SELECT 'GROUP BY ALL with tuple and CUBE', count()
FROM
(
    SELECT (a, b), count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY ALL WITH CUBE
);

SELECT 'GROUP BY ALL with tuple and ROLLUP', count()
FROM
(
    SELECT (a, b), count()
    FROM
    (
        SELECT number AS a, toString(number) AS b
        FROM numbers(3)
    )
    GROUP BY ALL WITH ROLLUP
);
