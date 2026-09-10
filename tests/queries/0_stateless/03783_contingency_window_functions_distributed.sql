-- Tags: distributed

SET distributed_aggregation_memory_efficient = 1;

SELECT
    round(any(cv), 4),
    round(any(cvb), 4),
    round(any(u), 4),
    round(any(cc), 4)
FROM
(
    SELECT
        number,
        cramersV(number % 10_000, number % 70_000) OVER () AS cv,
        cramersVBiasCorrected(number % 10_000, number % 70_000) OVER () AS cvb,
        theilsU(number % 10_000, number % 70_000) OVER () AS u,
        contingency(number % 10_000, number % 70_000) OVER () AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    round(argMax(cv, number), 4),
    round(argMax(cvb, number), 4),
    round(argMax(u, number), 4),
    round(argMax(cc, number), 4)
FROM
(
    SELECT
        number,
        cramersV(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cv,
        cramersVBiasCorrected(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cvb,
        theilsU(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS u,
        contingency(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);


SELECT
    arraySort(groupUniqArray((part, round(cv, 4)))),
    arraySort(groupUniqArray((part, round(cvb, 4)))),
    arraySort(groupUniqArray((part, round(u, 4)))),
    arraySort(groupUniqArray((part, round(cc, 4))))
FROM
(
    SELECT
        number % 7 AS part,
        cramersV(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cv,
        cramersVBiasCorrected(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cvb,
        theilsU(number % 10, number % 5) OVER (PARTITION BY number % 7) AS u,
        contingency(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    uniqExact(part),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cv, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cvb, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(u, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cc, 4))))
FROM
(
    SELECT
        part,
        any(cv)  AS cv,
        any(cvb) AS cvb,
        any(u)   AS u,
        any(cc)  AS cc
    FROM
    (
        SELECT
            number % 70_000 AS part,
            cramersV(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cv,
            cramersVBiasCorrected(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cvb,
            theilsU(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS u,
            contingency(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cc
        FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
    )
    GROUP BY part
);


SET distributed_aggregation_memory_efficient = 0;

SELECT
    round(any(cv), 4),
    round(any(cvb), 4),
    round(any(u), 4),
    round(any(cc), 4)
FROM
(
    SELECT
        number,
        cramersV(number % 10_000, number % 70_000) OVER () AS cv,
        cramersVBiasCorrected(number % 10_000, number % 70_000) OVER () AS cvb,
        theilsU(number % 10_000, number % 70_000) OVER () AS u,
        contingency(number % 10_000, number % 70_000) OVER () AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    round(argMax(cv, number), 4),
    round(argMax(cvb, number), 4),
    round(argMax(u, number), 4),
    round(argMax(cc, number), 4)
FROM
(
    SELECT
        number,
        cramersV(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cv,
        cramersVBiasCorrected(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cvb,
        theilsU(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS u,
        contingency(number % 10_000, number % 70_000)
            OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    arraySort(groupUniqArray((part, round(cv, 4)))),
    arraySort(groupUniqArray((part, round(cvb, 4)))),
    arraySort(groupUniqArray((part, round(u, 4)))),
    arraySort(groupUniqArray((part, round(cc, 4))))
FROM
(
    SELECT
        number % 7 AS part,
        cramersV(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cv,
        cramersVBiasCorrected(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cvb,
        theilsU(number % 10, number % 5) OVER (PARTITION BY number % 7) AS u,
        contingency(number % 10, number % 5) OVER (PARTITION BY number % 7) AS cc
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    uniqExact(part),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cv, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cvb, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(u, 4)))),
    groupBitXor(sipHash64(part, reinterpretAsUInt64(round(cc, 4))))
FROM
(
    SELECT
        part,
        any(cv)  AS cv,
        any(cvb) AS cvb,
        any(u)   AS u,
        any(cc)  AS cc
    FROM
    (
        SELECT
            number % 70_000 AS part,
            cramersV(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cv,
            cramersVBiasCorrected(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cvb,
            theilsU(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS u,
            contingency(number % 10_001, number % 70_001) OVER (PARTITION BY number % 70_000) AS cc
        FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
    )
    GROUP BY part
);
