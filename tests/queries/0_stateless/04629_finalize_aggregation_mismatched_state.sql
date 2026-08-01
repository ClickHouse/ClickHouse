-- Finalizing a column of aggregate states whose *actual* function finalizes to a different type
-- than the column's declared type (e.g. `quantileState` vs `quantilesState`, which share a state
-- representation) must raise a normal error, not an internal logical error / abort.
-- https://github.com/ClickHouse/ClickHouse/issues/111193

-- Repro 1: states of different functions brought together by a UNION (materialized branch).
-- Reproduces under both the analyzer and `enable_analyzer = 0`.
SELECT finalizeAggregation(s) FROM
(
    SELECT quantileState(number) AS s FROM numbers(7)
    UNION ALL
    SELECT quantilesState(0.9)(number) FROM numbers(5)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Repro 2 (from the issue): the non-UNION `arrayReduce`/`DISTINCT` path, which exercises different
-- plumbing (the const aggregate-state / header path). It only reproduces with the analyzer, so it is
-- pinned to `enable_analyzer = 1` to stay deterministic under the old-analyzer and random-settings jobs.
SELECT DISTINCT finalizeAggregation(s) FROM
(
    SELECT DISTINCT *, arrayReduce('quantilesState(0.9)', [65537]) AS s
    FROM (SELECT DISTINCT arrayReduce('quantileState(0.5)', [1048577]) AS s, 5e-324)
    LIMIT 1
)
SETTINGS enable_analyzer = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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

-- runningAccumulate shares the same header/runtime split and must reject the mismatch too.
SET allow_deprecated_error_prone_window_functions = 1;

SELECT runningAccumulate(s) FROM
(
    SELECT quantileState(number) AS s FROM numbers(7)
    UNION ALL
    SELECT quantilesState(0.9)(number) FROM numbers(5)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- runningAccumulate is order-dependent, so assert only that a homogeneous state column still
-- finalizes without error (row count), not the exact accumulated values, to stay deterministic
-- under randomized settings.
SELECT '-- runningAccumulate over a homogeneous state column still works';
SELECT count() FROM (SELECT runningAccumulate(s) FROM (SELECT sumState(number) AS s FROM numbers(5) GROUP BY number));
