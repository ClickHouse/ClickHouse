-- Regression test: `-State` combinator must not be applied to an aggregate
-- function that already returns a state (directly via another `-State`, or
-- transitively through combinators that propagate `isState()` such as
-- `-OrDefault`, `-OrNull`, `-If`, etc.).

SELECT uniqStateState(number) FROM numbers(1); -- { serverError ILLEGAL_AGGREGATION }

SELECT uniqHLL12StateOrDefaultState(number) FROM numbers(1); -- { serverError ILLEGAL_AGGREGATION }

SELECT uniqStateOrNullState(number) FROM numbers(1); -- { serverError ILLEGAL_AGGREGATION }

-- The original AST-fuzzer query that triggered use-after-free in
-- `~ColumnAggregateFunction` via `runningAccumulate` is now rejected up
-- front, well before `runningAccumulate` runs.
SELECT
    item,
    runningAccumulate(state, grouping),
    grouping
FROM
(
    SELECT
        number % 256 AS grouping,
        number AS item,
        uniqHLL12StateOrDefaultState(number) AS state
    FROM
    (
        SELECT number FROM system.numbers LIMIT 30
    )
    GROUP BY item
    ORDER BY grouping ASC NULLS FIRST, item DESC NULLS LAST
)
LIMIT 230
FORMAT Null
SETTINGS allow_deprecated_error_prone_window_functions = 1; -- { serverError ILLEGAL_AGGREGATION }

-- Standard usage is unaffected: `runningAccumulate(uniqState(x))`
SELECT runningAccumulate(s)
FROM
(
    SELECT uniqState(number) AS s
    FROM numbers(20)
    GROUP BY number % 3
    ORDER BY number % 3
)
SETTINGS allow_deprecated_error_prone_window_functions = 1;

-- Docs example for `runningAccumulate`
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(5)
SETTINGS allow_deprecated_error_prone_window_functions = 1;
