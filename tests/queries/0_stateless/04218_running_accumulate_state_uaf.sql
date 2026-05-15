-- Regression test: when `runningAccumulate` is called on a column whose
-- aggregate function returns its own state (i.e. is wrapped in `-State`,
-- possibly through `-OrDefault`/`-OrNull`/`-If`/`-ForEach`/etc.), the
-- function would historically push raw pointers to its stack-local
-- accumulator into the result `ColumnAggregateFunction`. That produced a
-- use-after-free at the result column's destruction time (caught by ASan)
-- and a use-of-uninitialized-value (MSan) in the AST fuzzer.
--
-- The fix in `src/Functions/runningAccumulate.cpp` detects the
-- state-returning case, allocates per-row state slots in an `Arena`,
-- deep-copies the running accumulator into them, and attaches the arena
-- to the result column via `ColumnAggregateFunction::addArena` so the
-- slots outlive the function call.

SET allow_deprecated_error_prone_window_functions = 1;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

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

SELECT number, finalizeAggregation(runningAccumulate(sumStateOrDefaultState(number)))
FROM numbers(5)
GROUP BY 1
ORDER BY number;

-- `-ForEach` over a state-returning nested function: the result column is a
-- `ColumnArray` whose inner column is a `ColumnAggregateFunction`. The arena
-- attachment walks the column tree (via `forEachMutableSubcolumnRecursively`)
-- so the inner column also retains the per-row state slots.
SELECT number, finalizeAggregation(finalizeAggregation(arrayJoin(runningAccumulate(sumStateOrDefaultStateForEachState([number, number + 1])))))
FROM numbers(5)
GROUP BY 1
ORDER BY number;
