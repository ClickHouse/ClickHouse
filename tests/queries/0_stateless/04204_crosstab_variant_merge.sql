SET enable_analyzer = 1;

SELECT round(cramersVMerge(s.`AggregateFunction(cramersV, UInt8, UInt8)`), 4)
FROM
(
    SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    UNION ALL
    SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) AS s FROM numbers(100)
);

SELECT round(contingencyMerge(s.`AggregateFunction(contingency, UInt8, UInt8)`), 4)
FROM
(
    SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    UNION ALL
    SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) AS s FROM numbers(100)
);

SELECT round(cramersVBiasCorrectedMerge(s.`AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)`), 4)
FROM
(
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    UNION ALL
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS s FROM numbers(100)
);

SELECT round(theilsUMerge(s.`AggregateFunction(theilsU, UInt8, UInt8)`), 4)
FROM
(
    SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    UNION ALL
    SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) AS s FROM numbers(100)
);
