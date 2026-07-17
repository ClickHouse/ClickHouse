-- Regression test for a LOGICAL_ERROR
-- 'Equal values are not contiguous within the range assumed to be sorted'
-- in DistinctSortedStreamTransform.
--
-- applyActionsToSortDescription() used to bail out whenever the expression
-- above a Sorting step contained an ARRAY JOIN, keeping the input sort
-- description unchanged. When such an expression re-applies an ARRAY JOIN that
-- produces an output column named identically to the sorted input, the stream
-- is no longer sorted by that column, yet the stale sort property survived and
-- reached the DISTINCT step, which then ran distinct-in-order over unsorted
-- data. The correlated scalar subquery below is decorrelated into exactly this
-- shape (ORDER BY over an arrayJoin-derived column, re-array-joined above).

SELECT DISTINCT s
FROM
(
    SELECT DISTINCT arrayJoin(['x', 'y', '']) AS s
    WHERE isNotNull((SELECT DISTINCT s FROM (SELECT toFixedString(NULL, 16) ORDER BY s ASC NULLS LAST) LIMIT 1 SETTINGS optimize_distinct_in_order = 1))
)
ORDER BY s
SETTINGS optimize_distinct_in_order = 1;
