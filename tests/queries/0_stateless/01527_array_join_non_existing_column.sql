SELECT
    count(),
    sum(ns)
FROM
(
    SELECT intDiv(number, NULL) AS k
    FROM system.numbers_mt
    GROUP BY k
)
ARRAY JOIN ns; --{serverError 8}

SELECT sum(ns) FROM numbers(10) ARRAY JOIN ns LIMIT 1; --{serverError 8}

SELECT 1;
