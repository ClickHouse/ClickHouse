SET enable_analyzer = 1;

-- Chained set operations (EXCEPT / INTERSECT) and nested UNION ALL build a Variant that is itself cast to
-- another Variant (Variant -> Variant). When one branch produces the window state representation and another
-- the aggregation representation of the same aggregate function, they collapse into one variant slot by name.
-- The Variant -> Variant cast must convert the subcolumn to the destination representation instead of copying
-- it verbatim, otherwise a later read of the state (here via the -Merge combinator) reads the bytes with the
-- wrong layout and the server aborts. See createVariantToVariantWrapper.

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

-- Dynamic carrier of the same representation mismatch. A Dynamic identifies its stored types by name and
-- re-resolves the type from that name. Casting the window state representation to Dynamic must store it in
-- the canonical (name-resolved) representation, otherwise dynamicElement / the -Merge combinator read the
-- window bytes with the aggregation layout and the server aborts. See createVariantToDynamicWrapper.
SELECT round(cramersVBiasCorrectedMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(cramersVMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(cramersV, UInt8, UInt8)')), 4)
FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(contingencyMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(contingency, UInt8, UInt8)')), 4)
FROM (SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(theilsUMerge(dynamicElement(CAST(w AS Dynamic), 'AggregateFunction(theilsU, UInt8, UInt8)')), 4)
FROM (SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

-- Both representations collapse into one Dynamic slot: the merge path combines them (aggregation + window).
DROP TABLE IF EXISTS t_04498_dyn;
CREATE TABLE t_04498_dyn (d Dynamic) ENGINE = Memory;
INSERT INTO t_04498_dyn SELECT CAST(w AS Dynamic) FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);
INSERT INTO t_04498_dyn SELECT CAST(a AS Dynamic) FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS a FROM numbers(100));
SELECT round(cramersVBiasCorrectedMerge(dynamicElement(d, 'AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)')), 4) FROM t_04498_dyn;
DROP TABLE t_04498_dyn;

-- The state can be nested inside another type (Array). The representation-converting cast recurses into it,
-- both for the Dynamic carrier and for a plain Variant slot.
SELECT round(arrayReduce('cramersVBiasCorrectedMerge', dynamicElement(CAST([w] AS Dynamic), 'Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8))')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);

SELECT round(arrayReduce('cramersVBiasCorrectedMerge', variantElement(CAST([w] AS Variant(Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)))), 'Array(AggregateFunction(cramersVBiasCorrected, UInt8, UInt8))')), 4)
FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS w FROM numbers(100) LIMIT 1);
