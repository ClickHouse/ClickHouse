-- Finalizing a column of aggregate states whose *actual* function finalizes to a different type
-- than the column's declared type (e.g. `quantileState` vs `quantilesState`, which share a state
-- representation) must raise a normal error, not an internal logical error / abort.
-- https://github.com/ClickHouse/ClickHouse/issues/111193

-- Repro 1: brought together by a UNION.
SELECT finalizeAggregation(s) FROM
(
    SELECT quantileState(number) AS s FROM numbers(7)
    UNION ALL
    SELECT quantilesState(0.9)(number) FROM numbers(5)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Repro 2: without any UNION, via arrayReduce (from the issue).
SELECT DISTINCT finalizeAggregation(s) FROM
(
    SELECT DISTINCT *, arrayReduce('quantilesState(0.9)', [65537]) AS s
    FROM (SELECT DISTINCT arrayReduce('quantileState(0.5)', [1048577]) AS s, 5e-324)
    LIMIT 1
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Positive cases: legitimate finalization still works.
SELECT '-- single homogeneous state';
SELECT finalizeAggregation(quantileState(0.5)(number)) FROM numbers(11);

SELECT '-- UNION of the same function finalizes fine';
SELECT finalizeAggregation(s) FROM
(
    SELECT quantileState(0.5)(number) AS s FROM numbers(7)
    UNION ALL
    SELECT quantileState(0.5)(number) FROM numbers(5)
)
ORDER BY 1;

SELECT '-- arrayReduce of a single function';
SELECT finalizeAggregation(arrayReduce('quantileState(0.5)', [1, 2, 3]));

SELECT '-- a non-quantile state (sum) still finalizes';
SELECT finalizeAggregation(arrayReduce('sumState', [1, 2, 3]));
