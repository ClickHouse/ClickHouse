-- The final DISTINCT over a stream sorted by a prefix of the distinct columns must evaluate
-- max_rows_in_distinct before the LIMIT hint: when a single chunk both reaches the LIMIT and
-- exceeds the limit, distinct_overflow_mode = 'throw' has to raise SET_SIZE_LIMIT_EXCEEDED
-- instead of silently stopping as if the mode were 'break'.
--
-- Two UNION ALL branches of 15 distinct rows each keep every per-branch preliminary DISTINCT
-- under the limit, so only the final DISTINCT above the sort sees all 30 distinct rows, and
-- max_block_size = 30 delivers them in one chunk that reaches the LIMIT hint (30 >= 25) and
-- exceeds max_rows_in_distinct (30 > 25) at the same time.

SET optimize_distinct_in_order = 1;
SET max_threads = 1;
SET max_block_size = 30;
SET max_rows_in_distinct = 25;
SET distinct_overflow_mode = 'throw';

SELECT '-- the final distinct over the sorted stream is the only DistinctSortedStreamTransform in the plan';
SELECT count() FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT a, b FROM
    (
        SELECT number AS a, number AS b FROM numbers(15)
        UNION ALL
        SELECT 15 + number AS a, 15 + number AS b FROM numbers(15)
    ) ORDER BY a LIMIT 25
)
WHERE trimLeft(explain) = 'DistinctSortedStreamTransform';

SELECT '-- the size limit is exceeded by the chunk that also reaches the LIMIT hint: the query must throw';
SELECT DISTINCT a, b FROM
(
    SELECT number AS a, number AS b FROM numbers(15)
    UNION ALL
    SELECT 15 + number AS a, 15 + number AS b FROM numbers(15)
) ORDER BY a LIMIT 25
FORMAT Null; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- with the size limit satisfied, the LIMIT hint still stops reading and the query succeeds';
SELECT count() FROM
(
    SELECT DISTINCT a, b FROM
    (
        SELECT number AS a, number AS b FROM numbers(15)
        UNION ALL
        SELECT 15 + number AS a, 15 + number AS b FROM numbers(15)
    ) ORDER BY a LIMIT 25
)
SETTINGS max_rows_in_distinct = 30, query_plan_remove_redundant_sorting = 0;
