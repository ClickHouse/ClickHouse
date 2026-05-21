-- https://github.com/ClickHouse/ClickHouse/issues/101738
-- estimateCompressionRatio as a window function with growing frames should
-- accumulate data across rows. Previously, insertResultInto finalized the
-- compression buffer without persisting the accumulated sizes, so each row
-- only saw its own data and all prior contributions were lost.

-- Use strings of varying length so that each row's contribution has a
-- different compressed size, making the accumulation effect clearly visible.
-- Without the fix, all rows would show the ratio of their own single value.
-- With the fix, each row reflects all values from the frame start.
SELECT
    number,
    estimateCompressionRatio()(repeat('a', number * 100 + 1)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ratio
FROM numbers(5)
ORDER BY number;

-- Verify that the number of distinct ratios equals the number of rows
-- (i.e. the ratio changes on every row, proving accumulation works).
SELECT uniq(ratio) = 10
FROM (
    SELECT estimateCompressionRatio()(repeat('x', number * 50 + 1)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ratio
    FROM numbers(10)
);
