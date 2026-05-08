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

-- Same shape but with `sumStateOrDefaultStateDistinct` so the running cumulative
-- result is observable: without the fix the pointers would dangle, with it the states
-- are copied into the result column's arena and `finalizeAggregation` returns 0,1,3,6,10.
SELECT finalizeAggregation(runningAccumulate(sum_k)) AS res
FROM
(
    SELECT
        number AS k,
        sumStateOrDefaultStateDistinct(k) IGNORE NULLS AS sum_k
    FROM numbers(5)
    GROUP BY k
    ORDER BY k ASC
);
