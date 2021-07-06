SELECT
    count(),
    sum(ns)
FROM
(
    SELECT intDiv(number, NULL) AS k
    FROM system.numbers_mt
    GROUP BY k
)
ARRAY JOIN ns; -- { serverError 47 }
