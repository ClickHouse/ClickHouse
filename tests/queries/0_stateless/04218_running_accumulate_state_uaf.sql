-- Regression test: when `runningAccumulate` is called on a column whose
-- aggregate function returns its own state (i.e. is wrapped in `-State`,
-- possibly through `-OrDefault`/`-OrNull`/`-If`/`-ForEach`/`-Merge`/etc.),
-- the function would historically push raw pointers to its stack-local
-- accumulator into the result `ColumnAggregateFunction`. That produced a
-- use-after-free at the result column's destruction time (caught by ASan)
-- and a use-of-uninitialized-value (MSan) in the AST fuzzer.
--
-- The fix in `src/Functions/runningAccumulate.cpp` detects the
-- state-returning case and uses `insertMergeResultInto` instead, which
-- delegates through the combinator chain to
-- `AggregateFunctionState::insertMergeResultInto` —
-- `ColumnAggregateFunction::insertFrom` allocates a fresh state in the
-- column's own arena and merges the accumulator into it, so each row
-- owns its own state independent of the stack-local accumulator.

SET allow_deprecated_error_prone_window_functions = 1;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

-- `runningAccumulate` resets its state per block and accumulates across rows within a block
-- in the order it receives them. Stateful functions are now kept below a same-level `ORDER BY`
-- (they observe the rows in pre-sort order), so the result depends on the input stream layout.
-- To make the running sums deterministic regardless of the harness's randomized settings, run
-- single-threaded (a single stream) with a block large enough to hold every row (a single block).
-- This does not weaken the use-after-free coverage below: the fault triggers when the result
-- column is destroyed, independent of the thread count.
SET max_threads = 1;
SET max_block_size = 65536;

-- The exact AST-fuzzer query that originally triggered the
-- use-after-free. It must now run cleanly and produce non-empty output.
SELECT count()
FROM
(
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
);

-- Standard usage is unaffected: `runningAccumulate(uniqState(x))` still
-- produces correct running cardinalities.
SELECT g, runningAccumulate(s)
FROM
(
    SELECT number % 3 AS g, uniqState(number) AS s
    FROM numbers(20)
    GROUP BY g
    ORDER BY g
)
ORDER BY g;

-- Docs example for `runningAccumulate`.
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(5)
ORDER BY number;

SELECT g, finalizeAggregation(runningAccumulate(s))
FROM
(
    SELECT number % 3 AS g, uniqHLL12StateOrDefaultState(number) AS s
    FROM numbers(20)
    GROUP BY g
    ORDER BY g
)
ORDER BY g;

-- The rows must be sorted in a subquery before `runningAccumulate` is
-- applied: stateful functions are not moved past a same-level `ORDER BY`
-- (they see the rows in the pre-sort order, which for `GROUP BY` output
-- is not deterministic), so accumulating over unsorted aggregation output
-- would produce nondeterministic running sums.
SELECT number, finalizeAggregation(runningAccumulate(s))
FROM
(
    SELECT number, sumStateOrDefaultState(number) AS s
    FROM numbers(5)
    GROUP BY number
    ORDER BY number
)
ORDER BY number;

-- `-ForEach` over a state-returning nested function: the result column is a
-- `ColumnArray` whose inner column is a `ColumnAggregateFunction`.
-- `AggregateFunctionForEach::insertMergeResultInto` materializes each
-- per-element state into the inner column via the nested
-- `insertMergeResultInto`, which keeps the per-row states independent of
-- the stack-local accumulator.
-- `arrayJoin` expands each row's two-element array into two rows that tie on `number`, and the
-- `ORDER BY number` alone does not pin their relative order (previously the sort-lifting optimization
-- masked this by applying `arrayJoin` after the sort). Order by the accumulated value too so the
-- output is deterministic.
SELECT number, finalizeAggregation(finalizeAggregation(arrayJoin(runningAccumulate(s)))) AS v
FROM
(
    SELECT number, sumStateOrDefaultStateForEachState([number, number + 1]) AS s
    FROM numbers(5)
    GROUP BY number
    ORDER BY number
)
ORDER BY number, v;

-- A column with type `AggregateFunction(sumStateMerge, ...)` keeps the
-- `Merge` wrapper visible through `ColumnAggregateFunction::getAggregateFunction`,
-- so `runningAccumulate` sees `AggregateFunctionMerge(AggregateFunctionState(...))`
-- with `isState` propagated to true. Without an
-- `AggregateFunctionMerge::insertMergeResultInto` override this used to
-- throw `NOT_IMPLEMENTED`; with the override it delegates to the nested
-- `AggregateFunctionState::insertMergeResultInto`.
SELECT number, finalizeAggregation(runningAccumulate(s))
FROM
(
    SELECT
        number,
        cast(sumState(number), 'AggregateFunction(sumStateMerge, AggregateFunction(sumState, UInt64))') AS s
    FROM numbers(5)
    GROUP BY number
    ORDER BY number
)
ORDER BY number;
