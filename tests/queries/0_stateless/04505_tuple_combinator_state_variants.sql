-- The crosstab family (`cramersV`, `cramersVBiasCorrected`, `theilsU`, `contingency`) keeps
-- different in-memory state layouts for aggregation and window execution
-- (`AggregateFunctionStateVariant`), with `mergeStateFromDifferentVariant` converting between them.
-- The `-Tuple` combinator composes the variant machinery element-wise, so states produced by a
-- window computation merge in an aggregation context and produce exactly the plain function's
-- results. Elements without a window implementation (such as only-null placeholders) keep the same
-- representation in both variants and merge normally alongside converted elements.

SELECT 'window states merged in aggregation context, equal to plain';
WITH
    (SELECT cramersVMerge(s) FROM (SELECT cramersVState(number % 3, number % 5) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100))) AS plain_a,
    (SELECT cramersVMerge(s) FROM (SELECT cramersVState(number % 2, number % 7) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100))) AS plain_b
SELECT r.1 = plain_a, r.2 = plain_b
FROM (SELECT cramersVTupleMerge(s) AS r FROM (SELECT cramersVTupleState((number % 3, number % 2), (number % 5, number % 7)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100)));

SELECT 'theilsU uses its own window layout';
WITH
    (SELECT theilsUMerge(s) FROM (SELECT theilsUState(number % 3, number % 5) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100))) AS plain_u
SELECT tupleElement(theilsUTupleMerge(s), 1) = plain_u
FROM (SELECT theilsUTupleState(tuple(number % 3), tuple(number % 5)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100));

SELECT 'mixed placeholder and variant-aware elements';
WITH
    (SELECT cramersVMerge(s) FROM (SELECT cramersVState(number % 2, number % 7) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100))) AS plain_b
SELECT r.1, r.2 = plain_b
FROM (SELECT cramersVTupleMerge(s) AS r FROM (SELECT cramersVTupleState((NULL, number % 2), (number % 5, number % 7)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100)));

SELECT 'window states cast to the aggregation-typed spelling';
-- The two layouts finalize through different floating-point summation orders, so a result can
-- differ from its counterpart in the last bit; compare with a tolerance and count real mismatches.
SELECT countIf(abs(a - b) > 1e-9)
FROM (
    SELECT
        tupleElement(finalizeAggregation(CAST(s, 'AggregateFunction(cramersVTuple, Tuple(UInt8, UInt8), Tuple(UInt8, UInt8))')), 1) AS a,
        tupleElement(finalizeAggregation(s), 1) AS b
    FROM (SELECT cramersVTupleState((number % 3, number % 2), (number % 5, number % 7)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(100)));

SELECT 'errors';
SELECT theilsUTupleMerge(s) FROM (SELECT cramersVTupleState(tuple(number % 3), tuple(number % 5)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(s, 'AggregateFunction(theilsUTuple, Tuple(UInt8), Tuple(UInt8))') FROM (SELECT cramersVTupleState(tuple(number % 3), tuple(number % 5)) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS s FROM numbers(10)); -- { serverError CANNOT_CONVERT_TYPE }
