
-- Chained set operations (EXCEPT / INTERSECT) and nested UNION ALL build a Variant that is itself cast to
-- another Variant (Variant -> Variant). One branch holds a state returned by a window function and another
-- a state returned by plain aggregation of the same aggregate function; both have the same type name and
-- share one variant slot. The -Merge combinator must read every state of that slot and give the right result.

SELECT round(cramersVBiasCorrectedMerge(s.`AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)`), 4)
FROM
(
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    EXCEPT DISTINCT
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toInt128OrDefault(number % 65535)) AS s FROM numbers(100)
    EXCEPT DISTINCT
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 65535)) AS s FROM numbers(100)
);

SELECT round(cramersVMerge(s.`AggregateFunction(cramersV, UInt8, UInt8)`), 4)
FROM
(
    SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    EXCEPT DISTINCT
    SELECT cramersVState(toUInt8(number % 10), toInt128OrDefault(number % 65535)) AS s FROM numbers(100)
    EXCEPT DISTINCT
    SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 65535)) AS s FROM numbers(100)
);

SELECT round(contingencyMerge(s.`AggregateFunction(contingency, UInt8, UInt8)`), 4)
FROM
(
    SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    EXCEPT DISTINCT
    SELECT contingencyState(toUInt8(number % 10), toInt128OrDefault(number % 65535)) AS s FROM numbers(100)
    EXCEPT DISTINCT
    SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 65535)) AS s FROM numbers(100)
);

SELECT round(theilsUMerge(s.`AggregateFunction(theilsU, UInt8, UInt8)`), 4)
FROM
(
    SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
    EXCEPT DISTINCT
    SELECT theilsUState(toUInt8(number % 10), toInt128OrDefault(number % 65535)) AS s FROM numbers(100)
    EXCEPT DISTINCT
    SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 65535)) AS s FROM numbers(100)
);

-- Nested UNION ALL: the inner UNION ALL already yields a Variant, the outer UNION ALL casts it Variant -> Variant.
SELECT round(cramersVBiasCorrectedMerge(s.`AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)`), 4)
FROM
(
    SELECT s FROM
    (
        SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS s FROM numbers(100) LIMIT 1
        UNION ALL
        SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toInt128OrDefault(number % 65535)) AS s FROM numbers(100)
    )
    UNION ALL
    SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS s FROM numbers(100)
);

-- The same through Dynamic. A Dynamic identifies its stored types by name. A state returned by a window
-- function that is cast to Dynamic must be readable through dynamicElement and the -Merge combinator.
SELECT round(cramersVBiasCorrectedMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(cramersVMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(cramersV, UInt8, UInt8)')), 4)
FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(contingencyMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(contingency, UInt8, UInt8)')), 4)
FROM (SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(theilsUMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(theilsU, UInt8, UInt8)')), 4)
FROM (SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

-- States of plain aggregation and of a window function share one Dynamic slot, and -Merge combines them.
DROP TABLE IF EXISTS t_04498_dyn;
CREATE TABLE t_04498_dyn (d Dynamic) ENGINE = Memory;
INSERT INTO t_04498_dyn SELECT CAST(w AS Dynamic) FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);
INSERT INTO t_04498_dyn SELECT CAST(a AS Dynamic) FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS a FROM numbers(100));
SELECT round(cramersVBiasCorrectedMerge(dynamicElement(d, 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')), 4) FROM t_04498_dyn;
DROP TABLE t_04498_dyn;

-- The state can be nested inside another type (Array), both in a Dynamic and in a plain Variant slot.
SELECT round(arrayReduce('cramersVBiasCorrectedMerge', dynamicElement(CAST([w] AS Dynamic), 'Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8))')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(arrayReduce('cramersVBiasCorrectedMerge', variantElement(CAST([w] AS Variant(Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)))), 'Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8))')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);
