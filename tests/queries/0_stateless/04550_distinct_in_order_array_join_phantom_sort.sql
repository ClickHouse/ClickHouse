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

-- enable_analyzer = 1: the correlated scalar subquery is a new-analyzer-only feature;
-- the old analyzer rejects it with UNKNOWN_IDENTIFIER before this shape can form.
SELECT DISTINCT s
FROM
(
    SELECT DISTINCT arrayJoin(['x', 'y', '']) AS s
    WHERE isNotNull((SELECT DISTINCT s FROM (SELECT toFixedString(NULL, 16) ORDER BY s ASC NULLS LAST) LIMIT 1 SETTINGS optimize_distinct_in_order = 1))
)
ORDER BY s
SETTINGS optimize_distinct_in_order = 1, enable_analyzer = 1;

-- The scan over the DAG outputs must skip (not abort on) the unsupported ARRAY JOIN output:
-- an arrayJoin result listed before a preserved sorted key must not wipe the valid sort prefix
-- and silently disable the DISTINCT / LIMIT BY in-order optimizations for that key.
SELECT count() > 0 AS distinct_in_order_preserved
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT arrayJoin([1, 2]) AS x, a
    FROM (SELECT number AS a FROM numbers(5) ORDER BY a)
    SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1
)
WHERE explain ILIKE '%DistinctSortedStreamTransform%';

SELECT count() > 0 AS limit_by_in_order_preserved
FROM
(
    EXPLAIN PIPELINE
    SELECT arrayJoin([1, 2]) AS x, a
    FROM (SELECT number AS a FROM numbers(5) ORDER BY a)
    LIMIT 1 BY a
    SETTINGS optimize_read_in_order = 1
)
WHERE explain ILIKE '%LimitBySortedStreamTransform%';
