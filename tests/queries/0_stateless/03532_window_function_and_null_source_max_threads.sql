CREATE TABLE empty (n UInt64) ENGINE = MergeTree() ORDER BY n;

-- A query that reproduces the problem, it has a JOIN of two empty tables followed by some window functions.
-- Before the fix max_threads limit was lost and the resulting pipeline was resized multiple times multiplying the number of streams by 20
-- So the result of the EXPLAIN below looked like this:
--
--(Expression)
--ExpressionTransform × 160000
--  (Window)
--  Resize 400 → 160000
--    WindowTransform × 400
--      (Sorting)
--      MergeSortingTransform × 400
--        LimitsCheckingTransform × 400
--          PartialSortingTransform × 400
--            Resize × 400 400 → 1
--              ScatterByPartitionTransform × 400 1 → 400
--                (Expression)
--                ExpressionTransform × 400
--                  (Window)
--                  Resize 20 → 400
--                    WindowTransform × 20
--                      (Sorting)
--                      MergeSortingTransform × 20
--                        LimitsCheckingTransform × 20
--                          PartialSortingTransform × 20
--                            Resize × 20 20 → 1
--                              ScatterByPartitionTransform × 20 1 → 20
--                                (Expression)
--                                ExpressionTransform × 20
--                                  (Expression)
--                                  ExpressionTransform × 20
--                                    (Join)
--                                    SimpleSquashingTransform × 20
--                                      ColumnPermuteTransform × 20
--                                        JoiningTransform × 20 2 → 1
--                                          Resize 1 → 20
--                                            (Expression)
--                                            ExpressionTransform
--                                              (Expression)
--                                              ExpressionTransform
--                                                (ReadFromPreparedSource)
--                                                NullSource 0 → 1
--                                            (Expression)
--                                            Resize × 2 20 → 1
--                                              .....
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE
        WITH
            nums AS
            (
                SELECT
                    n1.n AS a,
                    n2.n AS b
                FROM empty AS n1, empty AS n2
                WHERE (n1.n % 7) = (n2.n % 5)
            ),
            window1 AS
            (
                SELECT
                    a,
                    lagInFrame(a, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lag
                FROM nums
            ),
            window2 AS
            (
                SELECT
                    lag,
                    leadInFrame(lag, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lead
                FROM window1
            )
        SELECT lead
        FROM window2
        SETTINGS max_threads = 20, enable_parallel_replicas=0
) WHERE explain LIKE '%Resize%' LIMIT 3;


-- Same query by with crazy max_threads
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE
        WITH
            nums AS
            (
                SELECT
                    n1.n AS a,
                    n2.n AS b
                FROM empty AS n1, empty AS n2
                WHERE (n1.n % 7) = (n2.n % 5)
            ),
            window1 AS
            (
                SELECT
                    a,
                    lagInFrame(a, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lag
                FROM nums
            ),
            window2 AS
            (
                SELECT
                    lag,
                    leadInFrame(lag, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lead
                FROM window1
            )
        SELECT lead
        FROM window2
        SETTINGS max_threads = 2000, enable_parallel_replicas=0
) WHERE explain LIKE '%Resize%' LIMIT 3; -- {serverError LIMIT_EXCEEDED}


SELECT 'Same query with max_threads = 300';
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE
        WITH
            nums AS
            (
                SELECT
                    n1.n AS a,
                    n2.n AS b
                FROM empty AS n1, empty AS n2
                WHERE (n1.n % 7) = (n2.n % 5)
            ),
            window1 AS
            (
                SELECT
                    a,
                    lagInFrame(a, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lag
                FROM nums
            ),
            window2 AS
            (
                SELECT
                    lag,
                    leadInFrame(lag, 1, a) OVER (PARTITION BY a ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lead
                FROM window1
            )
        SELECT lead
        FROM window2
        SETTINGS max_threads = 300, enable_parallel_replicas=0
) WHERE explain LIKE '%Resize%' LIMIT 1;

DROP TABLE empty;


