-- Regression test for use-of-uninitialized-value (heap deallocation) in `runningAccumulate`
-- when the input is wrapped with a `-State` combinator. The result column then holds
-- aggregate states; with `insertResultInto` (the buggy version), it received raw pointers
-- to a stack-scoped buffer that was destroyed before the column was consumed.

SET allow_deprecated_error_prone_window_functions = 1;

-- AST fuzzer reproducer (simplified): the `DISTINCT` consumes the result column and
-- triggers `updateHashWithValue` on the dangling pointers, which MSan flags as
-- use-of-uninitialized-value (see SingleValueData.cpp:180 / runningAccumulate.cpp:155).
SELECT DISTINCT runningAccumulate(sum_k) AS res
FROM
(
    SELECT
        number AS k,
        minStateOrDefaultStateDistinct(k) IGNORE NULLS AS sum_k
    FROM numbers(5)
    GROUP BY k
    ORDER BY k ASC
)
FORMAT `Null`;

-- Same shape but read the finalized values out so the column is materialized.
-- Without the fix, the result column held raw pointers to a stack-scoped buffer
-- that was already destroyed; `finalizeAggregation` over the freed states would
-- read garbage / crash. We only count the rows here because `runningAccumulate`
-- resets the accumulator on each new block, and CI randomizes `max_block_size`
-- and `max_threads` — the cumulative values themselves are not deterministic
-- across runs, but the count is.
SELECT count() FROM
(
    SELECT finalizeAggregation(runningAccumulate(sum_k)) AS res
    FROM
    (
        SELECT
            number AS k,
            sumStateOrDefaultStateDistinct(k) IGNORE NULLS AS sum_k
        FROM numbers(5)
        GROUP BY k
        ORDER BY k ASC
    )
);
